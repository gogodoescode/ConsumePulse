#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "../src/event_processor.hpp"

namespace {
// IEventRepository is an interface specifically so the processing path can
// be tested without a database — this fake just records what it was asked
// to save.
class FakeEventRepository : public IEventRepository {
public:
    void save(const Event& event) override { saved.push_back(event); }
    std::vector<Event> saved;
};

class FakeAlertObserver : public IAlertObserver {
public:
    void onAlert(const Alert& alert) override { alerts.push_back(alert); }
    std::vector<Alert> alerts;
};
}  // namespace

TEST_CASE("processEvent saves the event and publishes an alert on an out-of-range reading",
          "[event_processor]") {
    FakeEventRepository repo;
    FakeAlertObserver observer;
    AlertPublisher publisher;
    publisher.attach(&observer);
    auto handlers = buildHandlers();

    Event spike{"e1", "app-server-1", "app-server", "cpu_temp", 98.0, "C", "2026-01-01T00:00:00Z"};
    bool known = processEvent(spike, repo, publisher, handlers);

    REQUIRE(known);
    REQUIRE(repo.saved.size() == 1);
    REQUIRE(repo.saved[0].event_id == "e1");
    REQUIRE(observer.alerts.size() == 1);
    REQUIRE(observer.alerts[0].event_id == "e1");
}

TEST_CASE("processEvent saves the event but publishes nothing when the reading is in range",
          "[event_processor]") {
    FakeEventRepository repo;
    FakeAlertObserver observer;
    AlertPublisher publisher;
    publisher.attach(&observer);
    auto handlers = buildHandlers();

    Event normal{"e2", "sensor-hub-1", "sensor-hub", "humidity_pct", 50.0, "%", "2026-01-01T00:00:00Z"};
    bool known = processEvent(normal, repo, publisher, handlers);

    REQUIRE(known);
    REQUIRE(repo.saved.size() == 1);
    REQUIRE(observer.alerts.empty());
}

TEST_CASE("processEvent still saves an event with an unrecognized device type, but reports it as unknown",
          "[event_processor]") {
    FakeEventRepository repo;
    FakeAlertObserver observer;
    AlertPublisher publisher;
    publisher.attach(&observer);
    auto handlers = buildHandlers();

    Event unknownType{"e3", "toaster-1", "toaster", "temp", 500.0, "C", "2026-01-01T00:00:00Z"};
    bool known = processEvent(unknownType, repo, publisher, handlers);

    REQUIRE_FALSE(known);
    REQUIRE(repo.saved.size() == 1);
    REQUIRE(observer.alerts.empty());
}
