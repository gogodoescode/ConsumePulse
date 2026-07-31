#pragma once

#include <optional>
#include <sstream>
#include <string>

#include "models.hpp"

// Detection rule for one metric. Swappable per metric without touching
// callers — that's the point of Strategy here.
class IAnomalyStrategy {
public:
    virtual ~IAnomalyStrategy() = default;
    virtual std::optional<Alert> evaluate(const Event& event) const = 0;
};

// Flags a reading outside [min, max] as an alert.
class ThresholdStrategy : public IAnomalyStrategy {
public:
    ThresholdStrategy(double min, double max) : min_(min), max_(max) {}

    std::optional<Alert> evaluate(const Event& event) const override {
        if (event.value >= min_ && event.value <= max_) {
            return std::nullopt;
        }

        std::ostringstream message;
        message << event.metric << "=" << event.value << " outside expected range ["
                << min_ << ", " << max_ << "] for device " << event.device_id;

        Alert alert;
        alert.event_id = event.event_id;
        alert.device_id = event.device_id;
        alert.rule_name = "threshold";
        alert.severity = "critical";
        alert.message = message.str();
        return alert;
    }

private:
    double min_;
    double max_;
};
