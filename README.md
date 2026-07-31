# ConsumePulse

Distributed telemetry & alerting pipeline: Python producers simulate devices,
stream events to Kafka, a C++ consumer processes and persists them to
PostgreSQL, and fans out alerts via an Observer pattern.

See `ConsumePulse_Build_Guide.md` for full architecture, schema, and the
phased build plan.

## Phase 0 — Infra

```
docker compose up -d
```

Brings up Kafka (KRaft, single broker) and PostgreSQL 16. The `device-events`
topic (3 partitions) and the `devices` / `events` / `alerts` tables are
created automatically.

Verify:

```
make topic-test   # lists Kafka topics
make psql         # opens a psql shell against the consumepulse DB
```
