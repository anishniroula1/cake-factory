SHELL := /bin/sh

.PHONY: help setup test dev postgres local down logs psql

help:
	@echo "make dev       Build and run PostgreSQL and the API"
	@echo "make postgres  Run only PostgreSQL"
	@echo "make local     Run the API locally against Docker PostgreSQL"
	@echo "make test      Install development packages and run tests"
	@echo "make down      Stop containers and keep database data"
	@echo "make logs      Follow API and PostgreSQL logs"
	@echo "make psql      Open a PostgreSQL shell"

setup:
	uv sync --extra dev

test: setup
	uv run pytest -q

dev:
	docker compose up --build

postgres:
	docker compose up -d postgres

local: setup
	POSTGRES_HOST=localhost POSTGRES_PORT=5434 uv run uvicorn bucket_demo.main:app --reload --host 0.0.0.0 --port 8010

down:
	docker compose down

logs:
	docker compose logs -f api postgres

psql:
	docker compose exec postgres psql -U sentence_user -d sentence_matches
