SHELL := /bin/sh

.PHONY: help setup test dev local down logs

help:
	@echo "make dev       Build and run the SQS worker"
	@echo "make local     Run the SQS worker locally"
	@echo "make test      Run the test suite with coverage"

setup:
	uv sync --extra dev

test: setup
	uv run ruff check doc2vec_completed ta_db_connection ta_utils tests generate_project.py decode_project.py
	uv run pytest -q

dev:
	docker compose up --build

local: setup
	uv run python -m doc2vec_completed.opensearch.main

down:
	docker compose down

logs:
	docker compose logs -f worker
