#pragma once

#include <iostream>
#include <vector>

#include "models.hpp"
#include "repository.hpp"

// Decouples anomaly detection from what happens after: detection just calls
// notify(), and doesn't know or care who's listening.
class IAlertObserver {
public:
    virtual ~IAlertObserver() = default;
    virtual void onAlert(const Alert& alert) = 0;
};

class ConsoleAlertObserver : public IAlertObserver {
public:
    void onAlert(const Alert& alert) override {
        std::cout << "[ALERT] " << alert.severity << " " << alert.rule_name
                  << " device=" << alert.device_id << ": " << alert.message << "\n";
    }
};

class DbAlertObserver : public IAlertObserver {
public:
    explicit DbAlertObserver(IAlertRepository& repository) : repository_(repository) {}

    void onAlert(const Alert& alert) override { repository_.save(alert); }

private:
    IAlertRepository& repository_;
};

class AlertPublisher {
public:
    void attach(IAlertObserver* observer) { observers_.push_back(observer); }

    void notify(const Alert& alert) {
        for (auto* observer : observers_) {
            observer->onAlert(alert);
        }
    }

private:
    std::vector<IAlertObserver*> observers_;
};
