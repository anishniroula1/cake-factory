SHELL := /bin/sh

.PHONY: help setup test dev local down logs

help:
	@echo "make dev       Build and run the SQS worker"
	@echo "make local     Run the SQS worker locally"
	@echo "make test      Run tests with coverage"

setup:
	uv sync --extra dev

test: setup
	uv run ruff check sentence_similarity tests generate_project.py decode_project.py
	uv run pytest -q

dev:
	docker compose up --build

local: setup
	uv run python -m sentence_similarity.main

down:
	docker compose down

logs:
	docker compose logs -f worker
