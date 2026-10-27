SHELL := /bin/sh

.PHONY: setup test local kafka kafka-stop kafka-logs

KAFKA_COMPOSE := docker compose -f docker-compose-kafka.yaml
KAFKA_TOPIC := rfs.ata.data_migration.api.events

setup:
	uv sync --extra dev

test: setup
	uv run ruff check transform_completed ta_db_connection ta_utils tests generate_project.py decode_project.py
	uv run pytest -q

local: setup kafka
	uv run python -m transform_completed.opensearch.main

kafka:
	$(KAFKA_COMPOSE) up -d --wait kafka
	$(KAFKA_COMPOSE) exec -T kafka kafka-topics \
		--bootstrap-server localhost:9092 \
		--create \
		--if-not-exists \
		--topic $(KAFKA_TOPIC) \
		--partitions 1 \
		--replication-factor 1

kafka-stop:
	$(KAFKA_COMPOSE) stop kafka

kafka-logs:
	$(KAFKA_COMPOSE) logs -f kafka
