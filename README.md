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

## Phase 1 — Producer

Simulates 6 devices (3 `app-server`, 3 `sensor-hub`), each walking 2 metrics.
Values random-walk within a normal range; ~5% of ticks push a metric to a
fixed out-of-range spike value on purpose, so Phase 4's threshold strategy
has something to catch.

```
cd producer
python -m venv venv
./venv/Scripts/python.exe -m pip install -r requirements.txt
./venv/Scripts/python.exe simulate_devices.py
```

Verify well-formed messages are landing in the topic:

```
docker exec -it consumepulse-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic device-events --from-beginning
```

## Phase 2 — Consumer skeleton

C++ service connects to Kafka, subscribes to `device-events` as consumer
group `telemetry-processors`, and prints each raw message (partition,
offset, key, payload). No parsing or persistence yet — that's Phase 3.

Requires: CMake, a C++17 toolchain (MSVC or g++), and vcpkg (manifest mode
pulls in `librdkafka` automatically).

```
cmake -S consumer -B consumer/build -DCMAKE_TOOLCHAIN_FILE=%VCPKG_ROOT%/scripts/buildsystems/vcpkg.cmake
cmake --build consumer/build --config Release
```

Run it (with the producer running in another terminal):

```
./consumer/build/Release/consumer.exe
```

**Alternative: Docker.** If the native binary won't run (e.g. Windows Smart
App Control blocking a freshly-built, unsigned `.exe`), build/run it in a
container instead — same `vcpkg.json`/`CMakeLists.txt`, just a Linux target
via `consumer/Dockerfile`:

```
docker compose --profile consumer run --rm consumer
```

It joins the compose network and talks to Kafka at `kafka:29092`
(overridable via the `KAFKA_BOOTSTRAP_SERVERS` env var; native runs default
to `localhost:9092`). Not started by `docker compose up` by default — it's
behind the `consumer` profile.

Verify: printed output matches what `simulate_devices.py` sent.

## Phase 3 — Parsing + Repository

Each Kafka message is parsed into an `Event` struct and persisted via
`PostgresEventRepository::save()`. Idempotent: `event_id` has a UNIQUE
constraint, and `save()` does `INSERT ... ON CONFLICT (event_id) DO NOTHING`,
so redelivery of the same message never creates a duplicate row. The device
carried in each event (`device_id` + `device_type`) is upserted into
`devices` first, satisfying the FK on `events.device_id` — there's no
separate seeding step.

Rebuild (adds `nlohmann-json` + `libpqxx` to the vcpkg manifest):

```
cmake -S consumer -B consumer/build -DCMAKE_TOOLCHAIN_FILE=%VCPKG_ROOT%/scripts/buildsystems/vcpkg.cmake
cmake --build consumer/build --config Release
```

or via Docker: `docker compose --profile consumer run --rm consumer`
(`PG_CONN_STRING` env var, defaults to `postgresql://consumepulse:consumepulse@localhost:5432/consumepulse`
natively, wired to the `postgres` service in compose).

Verify:

```
make events
```

To prove idempotency, redeliver everything and confirm row count doesn't move:

```
docker exec -it consumepulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group telemetry-processors --reset-offsets --to-earliest --topic device-events --execute
```

then rerun the consumer and re-check `SELECT count(*) FROM events` — same
number as before, and `count(*) = count(DISTINCT event_id)`.

## Phase 4 — Factory + Strategy

`DeviceHandlerFactory::create(device_type)` returns a handler
(`AppServerHandler` / `SensorHubHandler`) that owns a `ThresholdStrategy` per
metric. Ranges match the producer's normal random-walk range exactly, so the
producer's occasional spikes are guaranteed to trip a threshold:

| device type  | metric         | range      |
|--------------|----------------|------------|
| app-server   | `cpu_temp`     | 40 – 75    |
| app-server   | `latency_ms`   | 20 – 150   |
| sensor-hub   | `cpu_temp`     | 20 – 45    |
| sensor-hub   | `humidity_pct` | 30 – 70    |

No persistence yet — an out-of-range reading just logs an `[ALERT]` line.
Phase 5 adds the Observer pattern and writes it to the `alerts` table.

Rebuild and run the same way as Phase 3 (`make consumer-build` /
`make consumer`, or the native CMake commands above).

Verify: with the producer running, watch consumer output for lines like

```
[ALERT] critical threshold device=app-server-1: cpu_temp=98 outside expected range [40, 75] for device app-server-1
```

## Phase 5 — Observer + alert persistence

`AlertPublisher` fans an alert out to attached `IAlertObserver`s —
`ConsoleAlertObserver` (the `[ALERT]` log line) and `DbAlertObserver`
(`PostgresAlertRepository::save()`, writing to the `alerts` table). Detection
in `ThresholdStrategy` doesn't know or care who's listening.

Rebuild and run the same way as Phase 3/4.

Verify: with the producer running, watch for `[ALERT]` lines, then

```
make query
```

and confirm the rows match what was logged.
