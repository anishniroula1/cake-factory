FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY sentence_migration ./sentence_migration
RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["uvicorn", "sentence_migration.main:app", "--host", "0.0.0.0", "--port", "8000"]

