#pragma once

#include <pqxx/pqxx>

#include "models.hpp"

// Business logic depends on this interface, not on libpqxx directly.
class IEventRepository {
public:
    virtual ~IEventRepository() = default;
    virtual void save(const Event& event) = 0;
};

class PostgresEventRepository : public IEventRepository {
public:
    explicit PostgresEventRepository(pqxx::connection& conn) : conn_(conn) {}

    // Idempotent: a device is upserted by id, and ON CONFLICT DO NOTHING on
    // event_id means redelivery of the same message never produces a
    // duplicate row.
    void save(const Event& event) override {
        pqxx::work txn(conn_);

        txn.exec_params(
            "INSERT INTO devices (device_id, device_type) VALUES ($1, $2) "
            "ON CONFLICT (device_id) DO NOTHING",
            event.device_id, event.device_type);

        txn.exec_params(
            "INSERT INTO events (event_id, device_id, metric, value, unit, event_timestamp) "
            "VALUES ($1, $2, $3, $4, $5, $6) "
            "ON CONFLICT (event_id) DO NOTHING",
            event.event_id, event.device_id, event.metric, event.value, event.unit,
            event.event_timestamp);

        txn.commit();
    }

private:
    pqxx::connection& conn_;
};

class IAlertRepository {
public:
    virtual ~IAlertRepository() = default;
    virtual void save(const Alert& alert) = 0;
};

class PostgresAlertRepository : public IAlertRepository {
public:
    explicit PostgresAlertRepository(pqxx::connection& conn) : conn_(conn) {}

    void save(const Alert& alert) override {
        pqxx::work txn(conn_);

        txn.exec_params(
            "INSERT INTO alerts (event_id, device_id, rule_name, severity, message) "
            "VALUES ($1, $2, $3, $4, $5)",
            alert.event_id, alert.device_id, alert.rule_name, alert.severity, alert.message);

        txn.commit();
    }

private:
    pqxx::connection& conn_;
};
