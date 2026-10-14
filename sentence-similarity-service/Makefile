SHELL := /bin/sh

TSP_ID ?= 8205
SOURCE_QUEUE_NAME := sentence-similarity-local.fifo
NEXT_QUEUE_NAME := case-similarity-local.fifo
SOURCE_QUEUE_URL := http://localhost:4566/000000000000/$(SOURCE_QUEUE_NAME)

.PHONY: help setup test ostest localstack sos run dev local down logs

help:
	@echo "make localstack       Start LocalStack and create the local FIFO queues"
	@echo "make local            Run the worker locally (recommended)"
	@echo "make sos TSP_ID=8205  Send a sample document message to the local queue"
	@echo "make run              Build and run the worker in Docker"
	@echo "make ostest           Run only the OpenSearch flow tests"
	@echo "make test             Run lint and the complete test suite with coverage"
	@echo "make logs             Follow the Docker worker logs"
	@echo "make down             Stop the Docker services"

setup:
	uv sync --extra dev

test: setup
	uv run ruff check similarity_completed ta_db_connection ta_utils tests generate_project.py decode_project.py
	uv run pytest -q

ostest: setup
	uv run pytest -q tests/opensearch

localstack:
	docker compose up -d --wait localstack
	docker compose exec -T localstack awslocal sqs create-queue --queue-name $(SOURCE_QUEUE_NAME) --attributes FifoQueue=true,ContentBasedDeduplication=true >/dev/null
	docker compose exec -T localstack awslocal sqs create-queue --queue-name $(NEXT_QUEUE_NAME) --attributes FifoQueue=true,ContentBasedDeduplication=true >/dev/null
	@echo "Local SQS queues are ready."

sos:
	docker compose exec -T localstack awslocal sqs send-message --queue-url $(SOURCE_QUEUE_URL) --message-body '{"tsp_id":"$(TSP_ID)"}' --message-group-id sentence-similarity --message-deduplication-id "$$(uuidgen)" >/dev/null
	@echo "Sent tsp_id=$(TSP_ID) to $(SOURCE_QUEUE_NAME)."

run: localstack
	docker compose up --build worker

dev: run

local: setup
	uv run python -m similarity_completed.opensearch.main

down:
	docker compose down

logs:
	docker compose logs -f worker
