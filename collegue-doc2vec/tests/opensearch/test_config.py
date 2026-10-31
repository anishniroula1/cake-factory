from dataclasses import replace

import pytest

from doc2vec_completed.opensearch.config import Config, _aws_hostname


class VaultSecretsFake:
    def __init__(self):
        self.values = {
            "pgUser": "user",
            "pgPassword": "password",
            "pgHost": "database-host",
            "pgDatabase": "database-name",
            "pgPort": "5432",
            "awsReceiveSqsQueueEndpoint": "https://sqs.example.com/doc2vec.fifo",
            "awsSqsSendEventQueueEndpoint": "https://sqs.example.com/state.fifo",
        }

    def get(self, name, default=None):
        return self.values.get(name, default)


def required_environment(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.setenv("OPENSEARCH_HOST", "https://search.example.com/")
    monkeypatch.setenv("AWS_LOCAL_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setenv(
        "SENTENCE_SIMILARITY_BACKFILL_QUEUE_URL",
        "https://sqs.example.com/similarity.fifo",
    )
    # Fixed service values come from constants, so old env values are ignored.
    monkeypatch.setenv("OPENSEARCH_OCCURRENCE_ALIAS", "ignored-occurrence")
    monkeypatch.setenv("AWS_SQS_VISIBILITY_TIMEOUT", "1")
    monkeypatch.setattr(
        "doc2vec_completed.opensearch.config.VaultSecrets",
        VaultSecretsFake,
    )


def test_config_reads_only_vault_and_environment_values(monkeypatch):
    required_environment(monkeypatch)

    config = Config.from_environment()

    assert (
        config.environment,
        config.opensearch_host,
        config.aws_local_endpoint_url,
        config.doc2vec_queue_url,
        config.sentence_similarity_backfill_queue_url,
        config.state_machine_queue_url,
    ) == (
        "local",
        "search.example.com",
        "http://localhost:4566",
        "https://sqs.example.com/doc2vec.fifo",
        "https://sqs.example.com/similarity.fifo",
        "https://sqs.example.com/state.fifo",
    )

    invalid_values = [
        replace(config, pg_db_user=""),
        replace(config, pg_db_password=""),
        replace(config, pg_db_host=""),
        replace(config, pg_db_database=""),
        replace(config, pg_db_port=0),
        replace(config, environment="int", dynamic_credential_file=""),
        replace(config, opensearch_iam_access_role="not-an-arn"),
        replace(config, opensearch_host=""),
        replace(config, doc2vec_queue_url=""),
        replace(config, state_machine_queue_url=None),
    ]
    for invalid in invalid_values:
        with pytest.raises(ValueError):
            invalid.validate()


def test_opensearch_host_accepts_https_or_a_bare_hostname():
    assert [_aws_hostname(value) for value in ("https://search.example.com/", "search.example.com", "")] == [
        "search.example.com",
        "search.example.com",
        "",
    ]

    with pytest.raises(ValueError):
        _aws_hostname("http://search.example.com")

