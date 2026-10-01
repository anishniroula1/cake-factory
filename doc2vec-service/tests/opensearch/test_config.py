from dataclasses import replace

import pytest

from doc2vec_completed.opensearch.config import Config, read_integer


class VaultSecretsFake:
    def __init__(self):
        self.values = {
            "pgUser": "user",
            "pgPassword": "password",
            "pgHost": "database-host",
            "pgDatabase": "database-name",
            "pgPort": "5432",
            "awsReceiveSqsQueueEndpoint": ("https://sqs.example.com/doc2vec.fifo"),
            "awsSqsSendEventQueueEndpoint": ("https://sqs.example.com/state.fifo"),
        }

    def get(self, name, default=None):
        return self.values.get(name, default)


def test_config_reads_vault_and_environment_values(monkeypatch):
    monkeypatch.setattr(
        "doc2vec_completed.opensearch.config.VaultSecrets",
        VaultSecretsFake,
    )
    monkeypatch.setenv("OPENSEARCH_HOST", "search.example.com")
    monkeypatch.setenv(
        "SENTENCE_SIMILARITY_BACKFILL_QUEUE_URL",
        "https://sqs.example.com/similarity.fifo",
    )

    config = Config.from_environment()

    assert config.pg_db_user == "user"
    assert config.pg_db_password == "password"
    assert config.pg_db_host == "database-host"
    assert config.pg_db_database == "database-name"
    assert config.pg_db_port == 5432
    assert config.doc2vec_queue_url.endswith("doc2vec.fifo")
    assert config.state_machine_queue_url.endswith("state.fifo")
    assert config.sentence_similarity_backfill_queue_url.endswith("similarity.fifo")
    assert config.thread_worker_count == 100
    assert config.worker_batch_size == 30
    assert config.vector_dimension == 512

    with pytest.raises(ValueError):
        replace(config, thread_worker_count=101).validate()
    with pytest.raises(ValueError):
        replace(config, vector_dimension=1024).validate()
    with pytest.raises(ValueError):
        replace(config, sqs_wait_seconds=21).validate()
    with pytest.raises(ValueError):
        replace(config, opensearch_host="").validate()
    with pytest.raises(ValueError):
        replace(config, doc2vec_queue_url="").validate()


def test_environment_integer_reader_rejects_bad_values(monkeypatch):
    monkeypatch.setenv("NUMBER", "word")
    with pytest.raises(ValueError):
        read_integer("NUMBER", 1)
    monkeypatch.setenv("NUMBER", "0")
    with pytest.raises(ValueError):
        read_integer("NUMBER", 1)
