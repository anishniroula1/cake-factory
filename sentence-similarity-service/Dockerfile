FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY similarity_completed ./similarity_completed
RUN pip install --no-cache-dir .

CMD ["python", "-m", "similarity_completed.opensearch.main"]
