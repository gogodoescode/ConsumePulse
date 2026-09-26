# ConsumePulse

A telemetry pipeline I built to get hands-on with Kafka and concurrent
systems design beyond the tutorial level: a fleet of simulated devices
streams metrics into Kafka, a multi-threaded C++ consumer processes them
concurrently, persists everything to Postgres, and fans out alerts when a
reading goes out of range. The interesting part isn't the plumbing — it's
making concurrent processing, ordering guarantees, and correct offset
commits all work together without silently losing or duplicating data.

## Architecture

```
[Python producer]  -->  [Kafka: device-events]  -->  [poller thread]
 6 devices, keyed          3 partitions                parse JSON,
 by device_id,                                         hash(device_id) % N
 occasional spikes                                            |
                                                                v
                                  +---------+---------+---------+---------+
                                  | queue 0 | queue 1 | queue 2 | queue 3 |
                                  +---------+---------+---------+---------+
                                       |         |         |         |
                                       v         v         v         v
                                   worker 0  worker 1  worker 2  worker 3
                                   (each owns its own pqxx::connection)
                                       |         |         |         |
                                       +---------+----+----+---------+
                                                      |
                                                mark(partition, offset)
                                                      v
                                          OffsetTracker (contiguous-prefix
                                          watermark, per partition)
                                                      |
                                          commitSync every 500ms
                                                      v
                                              +----------------+
                                              |   PostgreSQL    |
                                              | devices / events |
                                              |     / alerts     |
                                              +----------------+
```

Every event also runs through `ThresholdStrategy` (Strategy pattern, picked
via `DeviceHandlerFactory`) inside its worker; an out-of-range reading fans
out through `AlertPublisher` (Observer pattern) to a console log and a
Postgres write.

## Design decisions

**Keyed partitioning for per-device ordering.** The producer keys each
message by `device_id`, so Kafka guarantees ordering within a partition.
Kafka's ordering guarantee is per-partition, not global — the consumer has
to respect that or the guarantee is worthless.

**Sharding by device, not round-robin.** The consumer has one poller thread
and N worker threads (default 4, `WORKER_THREADS` env var). A naive
thread-pool fan-out would destroy per-device ordering, so instead each
event is routed by `hash(device_id) % N` to a fixed worker — every device's
events always land on the same worker, in arrival order, while different
devices process in parallel. Each worker owns its own `pqxx::connection`;
libpqxx connections aren't thread-safe, so sharing one across workers would
be a data race.

**Bounded queues for backpressure.** The channel between the poller and
each worker is a fixed-capacity blocking queue
([bounded_queue.hpp](consumer/src/bounded_queue.hpp)). If a worker is stuck
retrying a dead database, its queue fills up and `push()` blocks — the
poller naturally slows down instead of buffering an unbounded backlog in
memory.

**A watermark, not "commit whatever finished last."** With four workers
finishing out of order, message 100 can complete before message 97. Naively
committing offset 100 would let a crash-and-restart skip 97 — silent data
loss. [offset_tracker.hpp](consumer/src/offset_tracker.hpp) tracks, per
partition, the highest contiguous run of completed offsets and only ever
exposes that as safe to commit. With 97 outstanding and 98–100 done, it
commits nothing; the moment 97 lands, it jumps straight to 100. This is
also why every write has to be idempotent — anything redelivered above the
watermark after a crash needs to be harmless, not just retried.

**Idempotent writes, everywhere.** `events.event_id` and
`alerts (event_id, rule_name)` both carry unique constraints, and every
insert is `ON CONFLICT ... DO NOTHING`. This is what actually prevents
duplicate processing on redelivery — not the transport. (Earlier version of
this had a real bug here: the `alerts` table had no constraint at all, so a
crash between the alert write and the offset commit could double-insert an
alert on replay. `events` was protected the whole time; `alerts` wasn't. Now
both are.)

**Commit after the write, not before — and batched.** `enable.auto.commit`
is off. Offsets commit only once a message's DB write has actually
succeeded, via the watermark above, batched on a 500ms timer rather than
per message (a `commitSync()` is a blocking broker round-trip; doing it per
event caps throughput in the low hundreds/sec no matter how fast everything
else is). The wider redelivery window on crash is fine specifically because
writes are idempotent — that property is what pays for this optimization.

**Poison pills vs. transient failures, handled differently.** A message
that fails to parse as JSON will never parse no matter how many times it's
retried, so it's logged and its offset commits immediately — no point
blocking the partition on it forever. A database write failure is assumed
transient (a restart, a network blip): it's logged, the connection is
rebuilt, and the *same* message is retried until it succeeds. The consumer
never advances past a message it couldn't persist.

## Patterns, and why

| Pattern | Where | Why |
|---|---|---|
| Factory | [device_factory.hpp](consumer/src/device_factory.hpp) | `DeviceHandlerFactory::create(device_type)` — a new device type is a new handler class, nothing existing changes. |
| Strategy | [anomaly_strategy.hpp](consumer/src/anomaly_strategy.hpp) | `ThresholdStrategy` behind `IAnomalyStrategy` — detection rule is swappable per metric without touching the caller. |
| Observer | [alert_observer.hpp](consumer/src/alert_observer.hpp) | `AlertPublisher` fans an alert out to `ConsoleAlertObserver` and `DbAlertObserver`. Detection doesn't know or care who's listening. |
| Repository | [repository.hpp](consumer/src/repository.hpp) | Business logic depends on `IEventRepository`/`IAlertRepository`, not on libpqxx directly — dependency inversion, and it's what makes the fake-repository unit test possible. |

## Tech stack

| Layer | Choice |
|---|---|
| Broker | Apache Kafka, KRaft mode (no Zookeeper) |
| Producer | Python, `confluent-kafka` |
| Consumer | C++17, `librdkafka`, `libpqxx` 7.x, `nlohmann-json`, `std::thread` |
| Database | PostgreSQL 16 |
| Build | CMake + vcpkg (manifest mode) |
| Tests | Catch2, run automatically as part of the Docker build |
| Orchestration | docker-compose |

## Quickstart

```
docker compose --profile all up -d --build
```

Brings up Kafka, Postgres, the producer, and the consumer in one command.
Or bring pieces up individually:

```
docker compose up -d                       # Kafka + Postgres only
docker compose --profile producer up -d --build
docker compose --profile consumer up -d --build
```

Useful `make` targets: `up`, `all`, `down`, `psql`, `events` (recent
`events` rows), `query` (recent `alerts` rows), `consumer-build`,
`consumer`.

`WORKER_THREADS` (default 4) and `PG_CONN_STRING` /
`KAFKA_BOOTSTRAP_SERVERS` are configurable via environment variables on the
`consumer` service.

## Running the tests

```
docker compose --profile consumer build consumer
```

`ctest` runs as part of the image build — a failing test fails the build,
not just a separate CI step that's easy to ignore. Covers `ThresholdStrategy`
(including that its range boundaries are inclusive), `DeviceHandlerFactory`'s
unknown-device-type path, the event-processing path end-to-end against a
fake `IEventRepository` (no database required), and — the one that actually
matters — `OffsetTracker`'s contiguous-prefix watermark: stalls on the
oldest outstanding offset, collapses correctly once it lands, never
double-advances on a duplicate or stale completion, tracks partitions
independently, and a 200-trial randomized-completion-order property test
asserting the committed watermark never exceeds the true contiguous prefix.
That's the logic where a bug would mean silent data loss on restart with no
error anywhere, so it gets the most scrutiny.

## Failure-mode demos

These are the parts I'd actually walk someone through.

**Idempotent redelivery.** Force Kafka to redeliver everything already
processed and confirm nothing duplicates:

```
docker exec -it consumepulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group telemetry-processors \
  --reset-offsets --to-earliest --topic device-events --execute
```

Rerun the consumer, then check both tables — row counts stay flat, and
`count(*) = count(DISTINCT event_id)` for `events`,
`count(*) = count(DISTINCT (event_id, rule_name))` for `alerts`.

**Kill the database mid-stream.** With the producer and consumer running:

```
docker stop consumepulse-postgres
```

Every worker logs `DB write failed, offset not committed, retrying` and
keeps retrying — no crash. `kafka-consumer-groups --describe` shows
`CURRENT-OFFSET` frozen at the lowest in-flight offset across all workers
while `LOG-END-OFFSET` keeps climbing. Bring it back:

```
docker start consumepulse-postgres
```

and the consumer catches up to zero lag with no message lost or
duplicated.

## Throughput

Draining a 6,000-event backlog, events spread evenly across devices. Time
is `max(ingested_at) - min(ingested_at)` in Postgres, so consumer startup
and group join aren't counted. All containers on one host (AMD Ryzen 7
6800H, 8c/16t, Docker Desktop/WSL2); each row is the mean of 2 runs.
Reproduce with `bench/bench.sh <devices> <workers> <events>`.

| Workers | 6 devices | Speedup | 60 devices | Speedup |
|---|---|---|---|---|
| 1 | 438/s | 1.0× | 442/s | 1.0× |
| 2 | 616/s | 1.4× | 642/s | 1.5× |
| 3 | 530/s | 1.2× | 1,102/s | 2.5× |
| 4 | 622/s | 1.4× | 1,101/s | 2.5× |

**Why 6 devices barely scale: shard skew.** With 4 workers,
`std::hash(device_id) % 4` puts 4 of the 6 devices on one worker and
none on another (per-worker counts: 4,000 / 1,000 / 1,000 / 0). The
busiest worker does two-thirds of the work, so the ceiling is 1.5×, and
the measured 1.4× sits right under it.

**Why 3 workers is slower than 2: partition skew plus head-of-line
blocking.** The 6 keys also land unevenly on Kafka's 3 partitions (4 / 2 /
0), and the poller drains a backlog roughly one partition at a time. While
it reads partition 1, one worker has 3,000 of its events and another has
1,000; the poller blocks pushing into the full queue while the other
worker idles. Modeling time as the busiest worker's load within each
partition phase predicts every 6-device row within ~6%.

**Confirming it:** the same run with 60 devices (a 14 / 20 / 10 / 16
device split at 4 workers) reaches 2.5×. The remaining gap to the 3.0×
load ceiling is shared cost I haven't isolated yet: one Postgres server
behind all four connections, the single poller thread, and a global log
mutex every event passes through.

## Known limitations

- **Event + alert writes aren't atomically linked.** They're two separate
  transactions. The idempotency constraints make redelivery safe, but the
  cleaner fix would be threading a single `pqxx::work` through both writes
  so they commit together — removing the window instead of just making it
  harmless on retry. Didn't get to it here.
- **No rebalance listener.** The watermark tracker assumes one consumer
  instance owns all partitions for the run. A second instance joining the
  group mid-run isn't handled — that'd need a rebalance callback to reset
  tracker state per partition.
- **A DB outage long enough to exceed `session.timeout.ms`** could trigger
  a rebalance while a worker is blocked retrying, since that worker isn't
  polling during the retry. Fine at demo scale; a production version would
  heartbeat manually during long retries.
- One Postgres connection per worker, no pooling — fine at this scale, not
  how I'd do it past a handful of workers.

## What's next

- Thread the event/alert writes into one transaction.
- A second `IEventRepository` implementation behind the same interface —
  the abstraction exists specifically so a new sink is a drop-in, not a
  rewrite.
- A rebalance listener so the multi-instance case is actually handled, not
  just untested.
