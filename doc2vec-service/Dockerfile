FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY doc2vec_service ./doc2vec_service
RUN pip install --no-cache-dir .

CMD ["python", "-m", "doc2vec_service.main"]
