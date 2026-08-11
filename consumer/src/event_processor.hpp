#pragma once

#include <map>
#include <memory>
#include <string>

#include "alert_observer.hpp"
#include "device_factory.hpp"
#include "models.hpp"
#include "repository.hpp"

// The part of the pipeline with no I/O concerns of its own beyond what's
// injected — save the event, look up its device handler, evaluate the
// strategy for its metric, and publish an alert if one fires. Kept
// separate from main.cpp's Kafka/threading/retry plumbing so it can be
// tested with fakes instead of a real DB.
// Returns false if device_type isn't recognized (nothing to evaluate).
inline bool processEvent(const Event& event, IEventRepository& eventRepository,
                          AlertPublisher& alertPublisher,
                          const std::map<std::string, std::unique_ptr<IDeviceHandler>>& handlers) {
    eventRepository.save(event);

    auto it = handlers.find(event.device_type);
    if (it == handlers.end()) {
        return false;
    }
    if (const IAnomalyStrategy* strategy = it->second->strategyFor(event.metric)) {
        if (auto alert = strategy->evaluate(event)) {
            alertPublisher.notify(*alert);
        }
    }
    return true;
}

inline std::map<std::string, std::unique_ptr<IDeviceHandler>> buildHandlers() {
    std::map<std::string, std::unique_ptr<IDeviceHandler>> handlers;
    handlers["app-server"] = DeviceHandlerFactory::create("app-server");
    handlers["sensor-hub"] = DeviceHandlerFactory::create("sensor-hub");
    return handlers;
}
