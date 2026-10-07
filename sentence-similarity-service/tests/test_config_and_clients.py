from dataclasses import replace

import pytest

from sentence_similarity.config import Config, read_integer
from sentence_similarity.custom_logger import CustomLogger
from sentence_similarity.open_search_client import OpenSearchClient


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
    monkeypatch.setenv("OPENSEARCH_HOST", "search.example.com")
    monkeypatch.setenv("SENTENCE_SIMILARITY_QUEUE_URL", "source.fifo")
    monkeypatch.setenv("API_TRANSFORM_QUEUE_URL", "next.fifo")
    monkeypatch.setattr("sentence_similarity.config.VaultSecrets", Secrets)


def test_config_reads_service_defaults(monkeypatch):
    required_environment(monkeypatch)

    config = Config.from_environment()

    assert config.source_sentence_batch_size == 30
    assert config.thread_worker_count == 30
    assert config.vector_dimension == 512
    assert config.occurrence_alias == "sentence_occurrences"
    assert config.asylee_catalog_alias == "sentence_semantic_catalog_asylee"

    invalid_values = [
        replace(config, opensearch_host=""),
        replace(config, sentence_similarity_queue_url=""),
        replace(config, api_transform_queue_url=""),
        replace(config, occurrence_alias=""),
        replace(config, asylee_catalog_alias=""),
        replace(config, thread_worker_count=31),
        replace(config, vector_dimension=1024),
        replace(config, match_threshold=101),
        replace(config, tracer_threshold=0),
        replace(config, sqs_wait_seconds=21),
    ]
    for invalid in invalid_values:
        with pytest.raises(ValueError):
            invalid.validate()


def test_integer_reader_rejects_bad_values(monkeypatch):
    monkeypatch.setenv("NUMBER", "word")
    with pytest.raises(ValueError):
        read_integer("NUMBER", 1)
    monkeypatch.setenv("NUMBER", "0")
    with pytest.raises(ValueError):
        read_integer("NUMBER", 1)


def test_opensearch_client_uses_active_aws_credentials(
    monkeypatch,
    service_config,
):
    credentials = object()

    class Session:
        def get_credentials(self):
            return credentials

    captured = {}
    monkeypatch.setattr(
        "sentence_similarity.open_search_client.boto3.Session",
        lambda **values: Session(),
    )
    monkeypatch.setattr(
        "sentence_similarity.open_search_client.AWSV4SignerAuth",
        lambda *values: "auth",
    )
    monkeypatch.setattr(
        "sentence_similarity.open_search_client.OpenSearch",
        lambda **values: captured.update(values) or "client",
    )

    assert OpenSearchClient(service_config).create_client() == "client"
    assert captured["pool_maxsize"] == 1
    assert captured["max_retries"] == 5


def test_opensearch_client_can_assume_role(monkeypatch, service_config):
    service_config = replace(
        service_config,
        opensearch_iam_access_role="arn:aws:iam::1:role/opensearch",
    )

    class Sts:
        def assume_role(self, **values):
            return {
                "Credentials": {
                    "AccessKeyId": "key",
                    "SecretAccessKey": "secret",
                    "SessionToken": "token",
                }
            }

    class BaseSession:
        def client(self, name):
            return Sts()

    assumed_session = object()

    def session_factory(**values):
        if "aws_access_key_id" in values:
            return assumed_session
        return BaseSession()

    monkeypatch.setattr(
        "sentence_similarity.open_search_client.boto3.Session",
        session_factory,
    )
    assert OpenSearchClient(service_config)._session() is assumed_session


def test_opensearch_client_requires_credentials(monkeypatch, service_config):
    class Session:
        def get_credentials(self):
            return None

    monkeypatch.setattr(
        "sentence_similarity.open_search_client.boto3.Session",
        lambda **values: Session(),
    )
    with pytest.raises(RuntimeError):
        OpenSearchClient(service_config).create_client()


def test_custom_logger_forwards_each_log_level():
    class Logger:
        def __init__(self):
            self.calls = []

        def debug(self, message):
            self.calls.append(("debug", message))

        def info(self, message):
            self.calls.append(("info", message))

        def warning(self, message):
            self.calls.append(("warning", message))

        def error(self, message):
            self.calls.append(("error", message))

    logger = Logger()
    custom_logger = CustomLogger(logger=logger)
    custom_logger.debug("one")
    custom_logger.info("two")
    custom_logger.warning("three")
    custom_logger.error("four")

    assert [level for level, message in logger.calls] == [
        "debug",
        "info",
        "warning",
        "error",
    ]
