.PHONY: up down logs psql topic-test producer consumer query

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

psql:
	docker exec -it consumepulse-postgres psql -U consumepulse -d consumepulse

topic-test:
	docker exec -it consumepulse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

producer:
	python3 producer/simulate_devices.py

consumer:
	./consumer/build/consumer

query:
	docker exec -it consumepulse-postgres psql -U consumepulse -d consumepulse -c "SELECT * FROM alerts ORDER BY created_at DESC LIMIT 20;"
