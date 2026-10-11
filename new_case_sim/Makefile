SHELL := /bin/sh

.PHONY: setup test local kafka kafka-stop kafka-logs

KAFKA_TOPIC := rfs.ata.text_analytics.hit

setup:
	uv sync --extra dev

test: setup
	uv run ruff check case_similarity_service ta_db_connection tests generate_project.py
	uv run pytest -q

local: setup kafka
	uv run python -m case_similarity_service.opensearch.main

kafka:
	docker compose up -d --wait kafka
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--if-not-exists \
		--topic $(KAFKA_TOPIC) \
		--partitions 1 \
		--replication-factor 1

kafka-stop:
	docker compose stop kafka

kafka-logs:
	docker compose logs -f kafka
