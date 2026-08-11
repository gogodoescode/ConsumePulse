#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <pqxx/pqxx>

#include "alert_observer.hpp"
#include "bounded_queue.hpp"
#include "device_factory.hpp"
#include "event_processor.hpp"
#include "log.hpp"
#include "models.hpp"
#include "offset_tracker.hpp"
#include "repository.hpp"

namespace {
std::atomic<bool> running{true};
void handleSignal(int) { running.store(false, std::memory_order_relaxed); }

constexpr int kDefaultWorkerCount = 4;
constexpr std::size_t kQueueCapacity = 256;
constexpr auto kCommitInterval = std::chrono::milliseconds(500);

std::string envOr(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    return value ? value : fallback;
}

int workerCountFromEnv() {
    const char* value = std::getenv("WORKER_THREADS");
    if (!value) {
        return kDefaultWorkerCount;
    }
    int parsed = std::atoi(value);
    return parsed > 0 ? parsed : kDefaultWorkerCount;
}

Event parseEvent(const std::string& payload) {
    nlohmann::json j = nlohmann::json::parse(payload);
    Event event;
    event.event_id = j.at("event_id").get<std::string>();
    event.device_id = j.at("device_id").get<std::string>();
    event.device_type = j.at("device_type").get<std::string>();
    event.metric = j.at("metric").get<std::string>();
    event.value = j.at("value").get<double>();
    event.unit = j.value("unit", "");
    event.event_timestamp = j.at("event_timestamp").get<std::string>();
    return event;
}

// Blocks until a connection is established (or shutdown is requested). DB
// downtime should stall processing, not crash the consumer.
std::unique_ptr<pqxx::connection> connectWithRetry(const std::string& connString) {
    while (running.load(std::memory_order_relaxed)) {
        try {
            return std::make_unique<pqxx::connection>(connString);
        } catch (const std::exception& e) {
            logLine(std::cerr, std::string("Failed to connect to Postgres, retrying in 2s: ") + e.what());
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }
    return nullptr;
}

struct WorkItem {
    Event event;
    int32_t partition;
    int64_t offset;
};

// One worker per shard, each with its own pqxx::connection — libpqxx
// connections aren't thread-safe, so sharing one across workers would be a
// data race. Repositories/observers/handlers are built once and only
// rebuilt on reconnect, not per message.
void workerLoop(int workerId, BoundedQueue<WorkItem>& queue, const std::string& pgConnString,
                 OffsetTracker& tracker) {
    std::unique_ptr<pqxx::connection> conn = connectWithRetry(pgConnString);
    if (!conn) {
        return;  // shutdown requested before a connection was ever made
    }

    auto eventRepository = std::make_unique<PostgresEventRepository>(*conn);
    auto alertRepository = std::make_unique<PostgresAlertRepository>(*conn);
    ConsoleAlertObserver consoleObserver;
    auto dbObserver = std::make_unique<DbAlertObserver>(*alertRepository);
    AlertPublisher alertPublisher;
    alertPublisher.attach(&consoleObserver);
    alertPublisher.attach(dbObserver.get());

    std::map<std::string, std::unique_ptr<IDeviceHandler>> handlers = buildHandlers();

    WorkItem item;
    while (queue.pop(item)) {
        bool done = false;
        while (!done && running.load(std::memory_order_relaxed)) {
            try {
                bool knownDeviceType = processEvent(item.event, *eventRepository, alertPublisher, handlers);
                if (!knownDeviceType) {
                    std::ostringstream warn;
                    warn << "[worker " << workerId << "] unknown device_type '"
                         << item.event.device_type << "' for device " << item.event.device_id
                         << ", skipping anomaly check";
                    logLine(std::cerr, warn.str());
                }

                tracker.markComplete(item.partition, item.offset);
                done = true;
                std::ostringstream line;
                line << "[worker " << workerId << "] [partition " << item.partition << " offset "
                     << item.offset << "] saved event_id=" << item.event.event_id
                     << " device=" << item.event.device_id << " metric=" << item.event.metric
                     << " value=" << item.event.value;
                logLine(std::cout, line.str());
            } catch (const std::exception& e) {
                std::ostringstream line;
                line << "[worker " << workerId << "] [partition " << item.partition << " offset "
                     << item.offset << "] DB write failed, offset not committed, retrying: " << e.what();
                logLine(std::cerr, line.str());
                conn = connectWithRetry(pgConnString);
                if (!conn) {
                    return;  // shutdown requested mid-retry
                }
                eventRepository = std::make_unique<PostgresEventRepository>(*conn);
                alertRepository = std::make_unique<PostgresAlertRepository>(*conn);
                dbObserver = std::make_unique<DbAlertObserver>(*alertRepository);
                alertPublisher = AlertPublisher{};
                alertPublisher.attach(&consoleObserver);
                alertPublisher.attach(dbObserver.get());
            }
        }
    }
}

// Commits every partition whose contiguous watermark has advanced since
// the last call. Batched on a timer rather than per message — a
// commitSync() is a blocking broker round-trip, so doing it per event caps
// throughput in the low hundreds/sec regardless of how fast everything
// else is. The wider redelivery window on crash is fine because every
// write below is idempotent.
void commitAdvanced(RdKafka::KafkaConsumer* consumer, OffsetTracker& tracker, const std::string& topic) {
    std::map<int32_t, int64_t> advanced = tracker.advancedSinceLastCommit();
    if (advanced.empty()) {
        return;
    }
    std::vector<RdKafka::TopicPartition*> offsets;
    for (const auto& [partition, offset] : advanced) {
        offsets.push_back(RdKafka::TopicPartition::create(topic, partition, offset));
    }
    RdKafka::ErrorCode err = consumer->commitSync(offsets);
    if (err != RdKafka::ERR_NO_ERROR) {
        logLine(std::cerr, std::string("Commit failed: ") + RdKafka::err2str(err));
    }
    RdKafka::TopicPartition::destroy(offsets);
}
}  // namespace

int main() {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    const std::string topic = "device-events";
    std::string errstr;
    std::string bootstrapServers = envOr("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    std::string pgConnString = envOr(
        "PG_CONN_STRING", "postgresql://consumepulse:consumepulse@localhost:5432/consumepulse");
    int workerCount = workerCountFromEnv();

    OffsetTracker tracker;
    std::vector<std::unique_ptr<BoundedQueue<WorkItem>>> queues;
    std::vector<std::thread> workers;
    for (int i = 0; i < workerCount; ++i) {
        queues.push_back(std::make_unique<BoundedQueue<WorkItem>>(kQueueCapacity));
    }
    for (int i = 0; i < workerCount; ++i) {
        workers.emplace_back(workerLoop, i, std::ref(*queues[i]), pgConnString, std::ref(tracker));
    }

    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    conf->set("bootstrap.servers", bootstrapServers, errstr);
    conf->set("group.id", "telemetry-processors", errstr);
    conf->set("auto.offset.reset", "earliest", errstr);
    conf->set("enable.auto.commit", "false", errstr);

    std::unique_ptr<RdKafka::KafkaConsumer> consumer(RdKafka::KafkaConsumer::create(conf.get(), errstr));
    if (!consumer) {
        std::cerr << "Failed to create consumer: " << errstr << "\n";
        return 1;
    }

    RdKafka::ErrorCode err = consumer->subscribe({topic});
    if (err != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Failed to subscribe: " << RdKafka::err2str(err) << "\n";
        return 1;
    }

    std::ostringstream startupLine;
    startupLine << "Consumer started, group 'telemetry-processors', topic '" << topic << "', "
                << workerCount << " worker(s) sharded by device_id. Ctrl+C to stop.";
    logLine(std::cout, startupLine.str());

    std::hash<std::string> hasher;
    auto lastCommitCheck = std::chrono::steady_clock::now();

    while (running.load(std::memory_order_relaxed)) {
        std::unique_ptr<RdKafka::Message> msg(consumer->consume(1000));
        switch (msg->err()) {
            case RdKafka::ERR__TIMED_OUT:
                break;
            case RdKafka::ERR_NO_ERROR: {
                std::string payload(static_cast<const char*>(msg->payload()), msg->len());
                int32_t partition = msg->partition();
                int64_t offset = msg->offset();
                tracker.noteSeen(partition, offset);

                try {
                    Event event = parseEvent(payload);
                    std::size_t shard = hasher(event.device_id) % queues.size();
                    queues[shard]->push(WorkItem{std::move(event), partition, offset});
                } catch (const std::exception& e) {
                    // A poison pill will never parse no matter how often we
                    // retry it, so log and let it complete immediately —
                    // there's no worker write to wait on.
                    std::ostringstream line;
                    line << "[partition " << partition << " offset " << offset
                         << "] skipping malformed message: " << e.what();
                    logLine(std::cerr, line.str());
                    tracker.markComplete(partition, offset);
                }
                break;
            }
            default:
                logLine(std::cerr, std::string("Consume error: ") + msg->errstr());
                break;
        }

        auto now = std::chrono::steady_clock::now();
        if (now - lastCommitCheck >= kCommitInterval) {
            commitAdvanced(consumer.get(), tracker, topic);
            lastCommitCheck = now;
        }
    }

    std::cout << "Shutting down, draining workers...\n";
    for (auto& queue : queues) {
        queue->shutdown();
    }
    for (auto& worker : workers) {
        worker.join();
    }
    commitAdvanced(consumer.get(), tracker, topic);  // flush whatever finished before shutdown

    consumer->close();
    return 0;
}
