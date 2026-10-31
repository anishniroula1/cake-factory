from dataclasses import replace

import pytest

from similarity_completed.opensearch.config import Config, _aws_hostname


class Secrets:
    def get(self, name, default=None):
        values = {
            "pgUser": "user",
            "pgPassword": "password",
            "pgHost": "database",
            "pgDatabase": "sentence",
            "pgPort": 5432,
        }
        return values.get(name, default)


def required_environment(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.setenv("OPENSEARCH_HOST", "https://search.example.com/")
    monkeypatch.setenv("AWS_LOCAL_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setenv("AWS_RECEIVE_SQS_ENDPOINT", "source.fifo")
    monkeypatch.setenv("AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT", "next.fifo")
    # These old environment values must not override the service constants.
    monkeypatch.setenv("OPENSEARCH_OCCURRENCE_ALIAS", "ignored-occurrence")
    monkeypatch.setenv("MATCH_THRESHOLD", "10")
    monkeypatch.setattr("similarity_completed.opensearch.config.VaultSecrets", Secrets)


def test_config_reads_vault_and_environment_values(monkeypatch):
    required_environment(monkeypatch)

    config = Config.from_environment()

    assert (config.environment, config.opensearch_host, config.receive_sqs_endpoint, config.send_sqs_endpoint) == (
        "local",
        "search.example.com",
        "source.fifo",
        "next.fifo",
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
        replace(config, receive_sqs_endpoint=""),
        replace(config, send_sqs_endpoint=""),
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
