import json, random, sys, uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from simulate_devices import METRICS_BY_DEVICE_TYPE, BOOTSTRAP_SERVERS, TOPIC

n_devices, n_messages = int(sys.argv[1]), int(sys.argv[2])
types = ["app-server", "sensor-hub"]
devices = [(f"{t}-{i}", t) for i in range(1, n_devices // 2 + 1) for t in types]

p = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "linger.ms": 20})
for k in range(n_messages):
    dev_id, dev_type = devices[k % len(devices)]
    spec = random.choice(METRICS_BY_DEVICE_TYPE[dev_type])
    value = spec["spike_value"] if random.random() < 0.05 else random.uniform(spec["min"], spec["max"])
    event = {
        "event_id": str(uuid.uuid4()), "device_id": dev_id, "device_type": dev_type,
        "metric": spec["metric"], "value": round(value, 2), "unit": spec["unit"],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    p.produce(TOPIC, key=dev_id.encode(), value=json.dumps(event).encode())
    if k % 1000 == 0:
        p.poll(0)
p.flush()
print(f"produced {n_messages} msgs across {len(devices)} devices")
