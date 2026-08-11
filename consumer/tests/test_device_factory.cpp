#include <catch2/catch_test_macros.hpp>

#include "../src/device_factory.hpp"

TEST_CASE("DeviceHandlerFactory creates the right handler per device type", "[device_factory]") {
    auto appServer = DeviceHandlerFactory::create("app-server");
    REQUIRE(appServer != nullptr);
    REQUIRE(appServer->strategyFor("cpu_temp") != nullptr);
    REQUIRE(appServer->strategyFor("latency_ms") != nullptr);
    REQUIRE(appServer->strategyFor("humidity_pct") == nullptr);

    auto sensorHub = DeviceHandlerFactory::create("sensor-hub");
    REQUIRE(sensorHub != nullptr);
    REQUIRE(sensorHub->strategyFor("cpu_temp") != nullptr);
    REQUIRE(sensorHub->strategyFor("humidity_pct") != nullptr);
    REQUIRE(sensorHub->strategyFor("latency_ms") == nullptr);
}

TEST_CASE("DeviceHandlerFactory returns nullptr for an unknown device type", "[device_factory]") {
    // Documents intent: an unrecognized device type silently produces no
    // handler rather than throwing. Callers (see event_processor.hpp) must
    // check for this explicitly instead of assuming create() always
    // succeeds.
    auto handler = DeviceHandlerFactory::create("toaster");
    REQUIRE(handler == nullptr);
}
