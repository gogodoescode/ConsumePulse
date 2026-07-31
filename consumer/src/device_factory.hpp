#pragma once

#include <map>
#include <memory>
#include <string>

#include "anomaly_strategy.hpp"

// One handler per device type; each owns the anomaly strategy for every
// metric that type reports. New device type = new handler class here,
// nothing existing changes.
class IDeviceHandler {
public:
    virtual ~IDeviceHandler() = default;

    const IAnomalyStrategy* strategyFor(const std::string& metric) const {
        auto it = strategies_.find(metric);
        return it != strategies_.end() ? it->second.get() : nullptr;
    }

protected:
    std::map<std::string, std::unique_ptr<IAnomalyStrategy>> strategies_;
};

class AppServerHandler : public IDeviceHandler {
public:
    AppServerHandler() {
        strategies_["cpu_temp"] = std::make_unique<ThresholdStrategy>(40.0, 75.0);
        strategies_["latency_ms"] = std::make_unique<ThresholdStrategy>(20.0, 150.0);
    }
};

class SensorHubHandler : public IDeviceHandler {
public:
    SensorHubHandler() {
        strategies_["cpu_temp"] = std::make_unique<ThresholdStrategy>(20.0, 45.0);
        strategies_["humidity_pct"] = std::make_unique<ThresholdStrategy>(30.0, 70.0);
    }
};

class DeviceHandlerFactory {
public:
    static std::unique_ptr<IDeviceHandler> create(const std::string& deviceType) {
        if (deviceType == "app-server") {
            return std::make_unique<AppServerHandler>();
        }
        if (deviceType == "sensor-hub") {
            return std::make_unique<SensorHubHandler>();
        }
        return nullptr;
    }
};
