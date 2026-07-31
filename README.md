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
