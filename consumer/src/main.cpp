#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include <librdkafka/rdkafkacpp.h>

namespace {
volatile sig_atomic_t running = 1;
void handleSignal(int) { running = 0; }
}  // namespace

int main() {
    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    std::string errstr;

    const char* bootstrapServersEnv = std::getenv("KAFKA_BOOTSTRAP_SERVERS");
    std::string bootstrapServers = bootstrapServersEnv ? bootstrapServersEnv : "localhost:9092";

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
                std::string key = msg->key() ? *msg->key() : "";
                std::string payload(static_cast<const char*>(msg->payload()), msg->len());
                std::cout << "[partition " << msg->partition() << " offset " << msg->offset()
                          << "] key=" << key << " value=" << payload << "\n";
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
