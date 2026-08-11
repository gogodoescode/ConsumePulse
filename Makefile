.PHONY: up all down logs psql topic-test producer consumer consumer-build test events query

up:
	docker compose up -d

all:
	docker compose --profile all up -d

down:
	docker compose --profile all down

logs:
	docker compose logs -f

psql:
	docker exec -it consumepulse-postgres psql -U consumepulse -d consumepulse

topic-test:
	docker exec -it consumepulse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

producer:
	python3 producer/simulate_devices.py

consumer-build:
	docker compose --profile consumer build consumer

consumer:
	docker compose --profile consumer run --rm consumer

test:
	docker compose --profile consumer build consumer

events:
	docker exec -it consumepulse-postgres psql -U consumepulse -d consumepulse -c "SELECT * FROM events ORDER BY ingested_at DESC LIMIT 20;"

query:
	docker exec -it consumepulse-postgres psql -U consumepulse -d consumepulse -c "SELECT * FROM alerts ORDER BY created_at DESC LIMIT 20;"
