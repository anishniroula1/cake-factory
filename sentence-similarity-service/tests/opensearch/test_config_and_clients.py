from dataclasses import replace

import pytest
from botocore.exceptions import NoCredentialsError

from similarity_completed.opensearch.config import Config, _aws_hostname
from similarity_completed.opensearch.open_search_client import OpenSearchClient


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


class ActiveCredentialsSession:
    def __init__(self, credentials):
        self.credentials = credentials

    def get_credentials(self):
        return self.credentials


class AssumeRoleStsClient:
    def assume_role(self, **values):
        return {"Credentials": {"AccessKeyId": "key", "SecretAccessKey": "secret", "SessionToken": "token"}}


class BaseRoleSession:
    def client(self, name):
        return AssumeRoleStsClient()


class MissingCredentialsSession:
    def get_credentials(self):
        return None


def required_environment(monkeypatch):
    monkeypatch.setenv("OPENSEARCH_HOST", "https://search.example.com/")
    monkeypatch.setenv("AWS_LOCAL_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setenv("AWS_RECEIVE_SQS_ENDPOINT", "source.fifo")
    monkeypatch.setenv("AWS_SQS_SENT_EVENT_QUEUE_ENDPOINT", "next.fifo")
    # These old environment values must not override the service constants.
    monkeypatch.setenv("OPENSEARCH_OCCURRENCE_ALIAS", "ignored-occurrence")
    monkeypatch.setenv("MATCH_THRESHOLD", "10")
    monkeypatch.setattr("similarity_completed.opensearch.config.VaultSecrets", Secrets)


def test_config_reads_service_defaults(monkeypatch):
    required_environment(monkeypatch)

    config = Config.from_environment()

    assert config.source_sentence_batch_size == 30
    assert config.thread_worker_count == 30
    assert config.vector_dimension == 512
    assert config.database_pool_size == 30
    assert config.database_max_overflow == 10
    assert config.opensearch_host == "search.example.com"
    assert config.occurrence_alias == "sentence_occurrences"
    assert config.asylee_catalog_alias == "sentence_semantic_catalog_asylee"
    assert config.refugee_catalog_alias == "sentence_semantic_catalog_refugee"
    assert config.tracer_alias == "tracer_form_language_catalog"
    assert config.receive_sqs_endpoint == "source.fifo"
    assert config.sent_sqs_endpoint == "next.fifo"
    assert config.aws_local_endpoint_url == "http://localhost:4566"
    assert config.match_threshold == 76
    assert config.tracer_threshold == 70
    assert config.ivf_nprobes == 64
    assert config.sqs_visibility_seconds == 7_200

    invalid_values = [
        replace(config, pg_db_user=""),
        replace(config, pg_db_password=""),
        replace(config, pg_db_host=""),
        replace(config, pg_db_database=""),
        replace(config, pg_db_port=0),
        replace(config, opensearch_iam_access_role="not-an-arn"),
        replace(config, opensearch_host=""),
        replace(config, receive_sqs_endpoint=""),
        replace(config, sent_sqs_endpoint=""),
    ]
    for invalid in invalid_values:
        with pytest.raises(ValueError):
            invalid.validate()


def test_opensearch_host_accepts_https_or_a_bare_hostname():
    assert _aws_hostname("https://search.example.com/") == "search.example.com"
    assert _aws_hostname("search.example.com") == "search.example.com"
    assert _aws_hostname("") == ""

    with pytest.raises(ValueError):
        _aws_hostname("http://search.example.com")


def test_opensearch_client_uses_active_aws_credentials(monkeypatch, service_config):
    credentials = object()

    captured = {}
    monkeypatch.setattr(
        "similarity_completed.opensearch.open_search_client.boto3.Session",
        lambda **values: ActiveCredentialsSession(credentials),
    )
    monkeypatch.setattr("similarity_completed.opensearch.open_search_client.AWSV4SignerAuth", lambda *values: "auth")
    monkeypatch.setattr(
        "similarity_completed.opensearch.open_search_client.OpenSearch",
        lambda **values: captured.update(values) or "client",
    )

    assert OpenSearchClient(service_config).create_client() == "client"
    assert captured["pool_maxsize"] == 1
    assert captured["max_retries"] == 5


def test_opensearch_client_can_assume_role(monkeypatch, service_config):
    service_config = replace(service_config, opensearch_iam_access_role="arn:aws:iam::1:role/opensearch")

    assumed_session = object()

    def session_factory(**values):
        if "aws_access_key_id" in values:
            return assumed_session
        return BaseRoleSession()

    monkeypatch.setattr("similarity_completed.opensearch.open_search_client.boto3.Session", session_factory)
    assert OpenSearchClient(service_config)._session() is assumed_session


def test_opensearch_client_requires_credentials(monkeypatch, service_config):
    monkeypatch.setattr(
        "similarity_completed.opensearch.open_search_client.boto3.Session", lambda **values: MissingCredentialsSession()
    )
    with pytest.raises(NoCredentialsError):
        OpenSearchClient(service_config).create_client()
