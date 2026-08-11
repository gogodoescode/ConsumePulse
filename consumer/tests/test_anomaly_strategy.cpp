#include <catch2/catch_test_macros.hpp>

#include "../src/anomaly_strategy.hpp"

TEST_CASE("ThresholdStrategy flags out-of-range readings", "[anomaly_strategy]") {
    ThresholdStrategy strategy(40.0, 75.0);

    Event inRange{"e1", "app-server-1", "app-server", "cpu_temp", 55.0, "C", "2026-01-01T00:00:00Z"};
    REQUIRE_FALSE(strategy.evaluate(inRange).has_value());

    Event tooHot{"e2", "app-server-1", "app-server", "cpu_temp", 98.0, "C", "2026-01-01T00:00:00Z"};
    auto alert = strategy.evaluate(tooHot);
    REQUIRE(alert.has_value());
    REQUIRE(alert->severity == "critical");
    REQUIRE(alert->rule_name == "threshold");
    REQUIRE(alert->event_id == "e2");
    REQUIRE(alert->device_id == "app-server-1");
}

TEST_CASE("ThresholdStrategy boundaries are inclusive", "[anomaly_strategy]") {
    ThresholdStrategy strategy(40.0, 75.0);

    Event atMin{"e1", "d1", "app-server", "cpu_temp", 40.0, "C", "2026-01-01T00:00:00Z"};
    Event atMax{"e2", "d1", "app-server", "cpu_temp", 75.0, "C", "2026-01-01T00:00:00Z"};
    Event belowMin{"e3", "d1", "app-server", "cpu_temp", 39.999, "C", "2026-01-01T00:00:00Z"};
    Event aboveMax{"e4", "d1", "app-server", "cpu_temp", 75.001, "C", "2026-01-01T00:00:00Z"};

    REQUIRE_FALSE(strategy.evaluate(atMin).has_value());
    REQUIRE_FALSE(strategy.evaluate(atMax).has_value());
    REQUIRE(strategy.evaluate(belowMin).has_value());
    REQUIRE(strategy.evaluate(aboveMax).has_value());
}
