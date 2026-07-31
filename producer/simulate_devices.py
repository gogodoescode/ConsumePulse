"""Simulates a small device fleet streaming telemetry into Kafka.

Two device types (app-server, sensor-hub), 3 devices each. Each metric does
a random walk within its normal range; occasionally a value is pushed
outside range on purpose so Phase 4's ThresholdStrategy has something to
catch.
"""
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "device-events"
TICK_SECONDS = 1.0
SPIKE_PROBABILITY = 0.05

DEVICES = [
    {"device_id": "app-server-1", "device_type": "app-server"},
    {"device_id": "app-server-2", "device_type": "app-server"},
    {"device_id": "app-server-3", "device_type": "app-server"},
    {"device_id": "sensor-hub-1", "device_type": "sensor-hub"},
    {"device_id": "sensor-hub-2", "device_type": "sensor-hub"},
    {"device_id": "sensor-hub-3", "device_type": "sensor-hub"},
]

# Normal range is what a random walk wanders within; spike_value is where we
# occasionally shove the reading to simulate an anomaly.
METRICS_BY_DEVICE_TYPE = {
    "app-server": [
        {"metric": "cpu_temp", "unit": "C", "min": 40, "max": 75, "step": 2.0, "spike_value": 98},
        {"metric": "latency_ms", "unit": "ms", "min": 20, "max": 150, "step": 8.0, "spike_value": 650},
    ],
    "sensor-hub": [
        {"metric": "cpu_temp", "unit": "C", "min": 20, "max": 45, "step": 1.5, "spike_value": 80},
        {"metric": "humidity_pct", "unit": "%", "min": 30, "max": 70, "step": 3.0, "spike_value": 3},
    ],
}


def init_state():
    """One random-walk value per (device_id, metric), seeded at range midpoint."""
    state = {}
    for device in DEVICES:
        for spec in METRICS_BY_DEVICE_TYPE[device["device_type"]]:
            midpoint = (spec["min"] + spec["max"]) / 2
            state[(device["device_id"], spec["metric"])] = midpoint
    return state


def next_value(current, spec):
    if random.random() < SPIKE_PROBABILITY:
        return spec["spike_value"]
    walked = current + random.uniform(-spec["step"], spec["step"])
    return max(spec["min"], min(spec["max"], walked))


def delivery_report(err, msg):
    if err is not None:
        print(f"[delivery-failed] {msg.key()}: {err}")


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    state = init_state()

    print(f"Producing to '{TOPIC}' for {len(DEVICES)} devices. Ctrl+C to stop.")
    try:
        while True:
            for device in DEVICES:
                metric_spec = random.choice(METRICS_BY_DEVICE_TYPE[device["device_type"]])
                key = (device["device_id"], metric_spec["metric"])
                value = next_value(state[key], metric_spec)
                state[key] = value

                event = {
                    "event_id": str(uuid.uuid4()),
                    "device_id": device["device_id"],
                    "device_type": device["device_type"],
                    "metric": metric_spec["metric"],
                    "value": round(value, 2),
                    "unit": metric_spec["unit"],
                    "event_timestamp": datetime.now(timezone.utc).isoformat(),
                }

                producer.produce(
                    TOPIC,
                    key=device["device_id"].encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                print(f"-> {event}")

            producer.poll(0)
            time.sleep(TICK_SECONDS)
    except KeyboardInterrupt:
        print("\nShutting down, flushing pending messages...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
