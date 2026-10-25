FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY doc_delete_completed ./doc_delete_completed
COPY ta_db_connection ./ta_db_connection
COPY ta_utils ./ta_utils
RUN uv sync --frozen --no-dev

CMD ["uv", "run", "python", "-m", "doc_delete_completed.opensearch.main"]
