CREATE TABLE devices (
    device_id       TEXT PRIMARY KEY,
    device_type     TEXT NOT NULL,       -- e.g. 'app-server', 'sensor-hub'
    display_name    TEXT,
    installed_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE events (
    event_id        UUID PRIMARY KEY,
    device_id       TEXT NOT NULL REFERENCES devices(device_id),
    metric          TEXT NOT NULL,        -- e.g. 'cpu_temp', 'latency_ms'
    value           DOUBLE PRECISION NOT NULL,
    unit            TEXT,
    event_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_events_device_time ON events(device_id, event_timestamp);

CREATE TABLE alerts (
    alert_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL REFERENCES events(event_id),
    device_id       TEXT NOT NULL REFERENCES devices(device_id),
    rule_name       TEXT NOT NULL,
    severity        TEXT NOT NULL,
    message         TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_alerts_device_time ON alerts(device_id, created_at);
