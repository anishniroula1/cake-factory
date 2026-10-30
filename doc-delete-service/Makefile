SHELL := /bin/sh

.PHONY: setup test local run localstack

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
