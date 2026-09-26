#!/usr/bin/env bash
# usage: bench.sh <devices> <workers> <messages>
set -euo pipefail
DEV=$1; W=$2; N=$3
SCR="$(cd "$(dirname "$0")" && pwd)"
PSQL="docker exec consumepulse-postgres psql -U consumepulse -d consumepulse -tAc"
KBIN=/opt/kafka/bin

docker rm -f bench-consumer >/dev/null 2>&1 || true
$PSQL "TRUNCATE alerts, events, devices" >/dev/null
docker exec consumepulse-kafka $KBIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group telemetry-processors --topic device-events --reset-offsets --to-latest --execute >/dev/null

docker run --rm -i --network consumepulse_default -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  consumepulse-producer python - "$DEV" "$N" < "$SCR/burst.py" >/dev/null

docker run -d --name bench-consumer --network consumepulse_default \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e PG_CONN_STRING=postgresql://consumepulse:consumepulse@postgres:5432/consumepulse \
  -e WORKER_THREADS="$W" consumepulse-consumer >/dev/null

for i in $(seq 1 600); do
  c=$($PSQL "SELECT count(*) FROM events")
  [ "$c" -ge "$N" ] && break
  sleep 1
done

secs=$($PSQL "SELECT extract(epoch FROM max(ingested_at) - min(ingested_at)) FROM events")
perworker=$(docker logs bench-consumer 2>/dev/null | grep -o '^\[worker [0-9]*\].*saved' | cut -d' ' -f2 | tr -d ']' | sort -n | uniq -c | awk '{printf "w%s=%s ", $2, $1}')
docker rm -f bench-consumer >/dev/null
awk -v d="$DEV" -v w="$W" -v n="$N" -v s="$secs" -v pw="$perworker" \
  'BEGIN { printf "devices=%-3s workers=%s events=%s secs=%.2f throughput=%.0f/s  %s\n", d, w, n, s, n/s, pw }'
