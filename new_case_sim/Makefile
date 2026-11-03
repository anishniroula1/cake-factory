SHELL := /bin/sh

.PHONY: setup test local kafka kafka-stop kafka-logs

KAFKA_TOPIC := rfs.ata.text_analytics.hit
KAFKA_COMPOSE := docker compose -f docker-compose-kafka.yaml

setup:
	uv sync --extra dev

test: setup
	uv run ruff check case_similarity_completed ta_db_connection tests generate_project.py
	uv run pytest -q

local: setup kafka
	uv run python -m case_similarity_completed.opensearch.main

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
	$(KAFKA_COMPOSE) stop kafka zookeeper

kafka-logs:
	$(KAFKA_COMPOSE) logs -f kafka zookeeper
