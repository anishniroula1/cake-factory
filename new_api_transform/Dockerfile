FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY api_transform_service ./api_transform_service
COPY ta_db_connection ./ta_db_connection
COPY ta_utils ./ta_utils
RUN pip install --no-cache-dir .

CMD ["python", "-m", "api_transform_service.opensearch.main"]

