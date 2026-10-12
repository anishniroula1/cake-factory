SHELL := /bin/sh

.PHONY: setup test local

setup:
	uv sync --extra dev

test: setup
	uv run ruff check api_transform_service ta_db_connection ta_utils tests generate_project.py decode_project.py
	uv run pytest -q

local: setup
	uv run python -m api_transform_service.opensearch.main

