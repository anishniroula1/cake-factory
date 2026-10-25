SHELL := /bin/sh

.PHONY: setup test local run localstack send-message

setup:
	uv sync --extra dev

test: setup
	uv run ruff check doc_delete_completed ta_db_connection ta_utils tests generate_project.py
	uv run pytest -q

local: setup
	uv run watchfiles --filter python 'python -m doc_delete_completed.opensearch.main' \
		doc_delete_completed ta_db_connection ta_utils

run:
	docker compose up --build document-delete

localstack:
	docker compose up -d --wait localstack

send-message: localstack
	aws --endpoint-url http://localhost:4566 sqs send-message \
		--queue-url http://localhost:4566/000000000000/document-delete-local.fifo \
		--message-body '{"source":"P2","tsp_id":8205,"sent_by":"api-transform"}' \
		--message-group-id local \
		--message-deduplication-id $$(date +%s)
