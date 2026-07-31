#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <pqxx/pqxx>

#include "alert_observer.hpp"
#include "device_factory.hpp"
#include "models.hpp"
#include "repository.hpp"

namespace {
volatile sig_atomic_t running = 1;
void handleSignal(int) { running = 0; }

std::string envOr(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    return value ? value : fallback;
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

// Blocks until a connection is established. DB downtime should stall
// processing, not crash the consumer.
std::unique_ptr<pqxx::connection> connectWithRetry(const std::string& connString) {
    while (running) {
        try {
            return std::make_unique<pqxx::connection>(connString);
        } catch (const std::exception& e) {
            std::cerr << "Failed to connect to Postgres, retrying in 2s: " << e.what() << "\n";
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }
    return nullptr;
}
}  // namespace

int main() {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    std::string errstr;
    std::string bootstrapServers = envOr("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    std::string pgConnString = envOr(
        "PG_CONN_STRING", "postgresql://consumepulse:consumepulse@localhost:5432/consumepulse");

    std::unique_ptr<pqxx::connection> conn = connectWithRetry(pgConnString);
    if (!conn) {
        return 1;  // only happens if a signal interrupted startup
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

    RdKafka::ErrorCode err = consumer->subscribe({"device-events"});
    if (err != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Failed to subscribe: " << RdKafka::err2str(err) << "\n";
        return 1;
    }

    std::cout << "Consumer started, group 'telemetry-processors', topic 'device-events'. Ctrl+C to stop.\n";

    while (running) {
        std::unique_ptr<RdKafka::Message> msg(consumer->consume(1000));
        switch (msg->err()) {
            case RdKafka::ERR__TIMED_OUT:
                break;
            case RdKafka::ERR_NO_ERROR: {
                std::string payload(static_cast<const char*>(msg->payload()), msg->len());

                Event event;
                try {
                    event = parseEvent(payload);
                } catch (const std::exception& e) {
                    // A poison pill will never parse no matter how often we
                    // retry it, so log, commit past it, and move on.
                    std::cerr << "[partition " << msg->partition() << " offset " << msg->offset()
                              << "] skipping malformed message: " << e.what() << "\n";
                    consumer->commitSync(msg.get());
                    break;
                }

                // A DB write failure is presumed transient (connection
                // drop, restart): retry the same message against a fresh
                // connection until it succeeds. The offset is only
                // committed on success, so a crash or restart mid-outage
                // redelivers this message instead of silently losing it.
                bool committed = false;
                while (!committed && running) {
                    try {
                        PostgresEventRepository eventRepository(*conn);
                        PostgresAlertRepository alertRepository(*conn);
                        ConsoleAlertObserver consoleObserver;
                        DbAlertObserver dbObserver(alertRepository);
                        AlertPublisher alertPublisher;
                        alertPublisher.attach(&consoleObserver);
                        alertPublisher.attach(&dbObserver);

                        eventRepository.save(event);

                        auto handler = DeviceHandlerFactory::create(event.device_type);
                        const IAnomalyStrategy* strategy =
                            handler ? handler->strategyFor(event.metric) : nullptr;
                        if (strategy) {
                            if (auto alert = strategy->evaluate(event)) {
                                alertPublisher.notify(*alert);
                            }
                        }

                        consumer->commitSync(msg.get());
                        committed = true;
                        std::cout << "[partition " << msg->partition() << " offset " << msg->offset()
                                  << "] saved event_id=" << event.event_id
                                  << " device=" << event.device_id << " metric=" << event.metric
                                  << " value=" << event.value << "\n";
                    } catch (const std::exception& e) {
                        std::cerr << "[partition " << msg->partition() << " offset " << msg->offset()
                                  << "] DB write failed, offset not committed, retrying: " << e.what()
                                  << "\n";
                        conn = connectWithRetry(pgConnString);
                    }
                }
                break;
            }
            default:
                std::cerr << "Consume error: " << msg->errstr() << "\n";
                break;
        }
    }

    std::cout << "Shutting down...\n";
    consumer->close();
    return 0;
}
