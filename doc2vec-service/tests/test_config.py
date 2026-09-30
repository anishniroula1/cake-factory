from dataclasses import replace

import pytest

from doc2vec_service.config import Config, readInteger


def baseEnvironment(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://user:pass@db/name")
    monkeypatch.setenv("OPENSEARCH_HOST", "search.example.com")
    monkeypatch.setenv("DOC2VEC_QUEUE_URL", "https://sqs.example.com/doc2vec.fifo")


def test_config_reads_defaults_and_validates(monkeypatch):
    baseEnvironment(monkeypatch)
    monkeypatch.setenv("OPENSEARCH_PORT", "9200")
    monkeypatch.setenv("OPENSEARCH_SERVICE", "not-es")
    monkeypatch.setenv("OPENSEARCH_BATCH_SIZE", "1")
    monkeypatch.setenv("WORKER_BATCH_SIZE", "1")
    monkeypatch.setenv("HISTORICAL_WORKER_COUNT", "1")
    monkeypatch.setenv("RETRY_SECONDS", "1")
    monkeypatch.setenv("TITAN_EMBEDDING_TYPE", "1")
    config = Config.fromEnvironment()
    assert config.pgDbUser == "user"
    assert config.pgDbPassword == "pass"
    assert config.pgDbHost == "db"
    assert config.pgDbDatabase == "name"
    assert config.pgDbPort == 5432
    assert config.opensearchPort == 443
    assert config.opensearchService == "es"
    assert config.opensearchBatchSize == 1_000
    assert config.historicalWorkerCount == 100
    assert config.workerBatchSize == 30
    assert config.retrySeconds == 5
    assert config.titanEmbeddingType == 2
    assert config.vectorDimension == 512

    with pytest.raises(ValueError):
        replace(config, historicalWorkerCount=101).validate()
    with pytest.raises(ValueError):
        replace(config, vectorDimension=1024).validate()
    with pytest.raises(ValueError):
        replace(config, sqsWaitSeconds=21).validate()
    with pytest.raises(ValueError):
        replace(config, databaseUrl="").validate()
    with pytest.raises(ValueError):
        replace(config, opensearchHost="").validate()
    with pytest.raises(ValueError):
        replace(config, sourceQueueUrl="").validate()


def test_environment_parsers_reject_bad_values(monkeypatch):
    monkeypatch.setenv("NUMBER", "word")
    with pytest.raises(ValueError):
        readInteger("NUMBER", 1)
    monkeypatch.setenv("NUMBER", "0")
    with pytest.raises(ValueError):
        readInteger("NUMBER", 1)
