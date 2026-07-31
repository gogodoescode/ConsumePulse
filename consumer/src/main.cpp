#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

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

    pqxx::connection conn(pgConnString);
    PostgresEventRepository eventRepository(conn);
    PostgresAlertRepository alertRepository(conn);

    ConsoleAlertObserver consoleObserver;
    DbAlertObserver dbObserver(alertRepository);
    AlertPublisher alertPublisher;
    alertPublisher.attach(&consoleObserver);
    alertPublisher.attach(&dbObserver);

    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    conf->set("bootstrap.servers", bootstrapServers, errstr);
    conf->set("group.id", "telemetry-processors", errstr);
    conf->set("auto.offset.reset", "earliest", errstr);

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
                try {
                    Event event = parseEvent(payload);
                    eventRepository.save(event);
                    std::cout << "[partition " << msg->partition() << " offset " << msg->offset()
                              << "] saved event_id=" << event.event_id
                              << " device=" << event.device_id << " metric=" << event.metric
                              << " value=" << event.value << "\n";

                    auto handler = DeviceHandlerFactory::create(event.device_type);
                    const IAnomalyStrategy* strategy =
                        handler ? handler->strategyFor(event.metric) : nullptr;
                    if (strategy) {
                        if (auto alert = strategy->evaluate(event)) {
                            alertPublisher.notify(*alert);
                        }
                    }
                } catch (const std::exception& e) {
                    std::cerr << "[partition " << msg->partition() << " offset " << msg->offset()
                              << "] skipped malformed message: " << e.what() << "\n";
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
