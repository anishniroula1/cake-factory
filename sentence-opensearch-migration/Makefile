SHELL := /bin/sh

.PHONY: help setup test dev local down logs

help:
	@echo "make dev    Build and run the migration API on port 8009"
	@echo "make local  Run the migration API locally on port 8009"
	@echo "make test   Install development packages and run tests"
	@echo "make down   Stop Docker containers"
	@echo "make logs   Follow API logs"

setup:
	uv sync --extra dev

test: setup
	uv run pytest -q

dev:
	docker compose up --build

local: setup
	uv run uvicorn sentence_migration.main:app --reload --host 0.0.0.0 --port 8009

down:
	docker compose down

logs:
	docker compose logs -f api

