from dataclasses import replace

import pytest

from sentence_similarity.config import Config, readInteger
from sentence_similarity.database import Database
from sentence_similarity.open_search_client import OpenSearchClient


def baseEnvironment(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://user:pass@db/name")
    monkeypatch.setenv("OPENSEARCH_HOST", "search.example.com")
    monkeypatch.setenv(
        "SENTENCE_SIMILARITY_QUEUE_URL",
        "https://sqs.example.com/source.fifo",
    )
    monkeypatch.setenv(
        "API_TRANSFORM_QUEUE_URL",
        "https://sqs.example.com/next.fifo",
    )


def test_config_reads_defaults_and_validates(monkeypatch):
    baseEnvironment(monkeypatch)
    config = Config.fromEnvironment()
    assert config.sourcePageSize == 100
    assert config.workerCount == 16
    assert config.sqsVisibilitySeconds == 7_200

    invalidValues = [
        replace(config, databaseUrl=""),
        replace(config, opensearchHost=""),
        replace(config, sourceQueueUrl=""),
        replace(config, nextQueueUrl=""),
        replace(config, workerCount=21),
        replace(config, vectorDimension=1024),
        replace(config, matchThreshold=101),
        replace(config, tracerThreshold=0),
        replace(config, sqsWaitSeconds=21),
    ]
    for invalid in invalidValues:
        with pytest.raises(ValueError):
            invalid.validate()


def test_environment_number_parser_rejects_bad_values(monkeypatch):
    monkeypatch.setenv("NUMBER", "word")
    with pytest.raises(ValueError):
        readInteger("NUMBER", 1)
    monkeypatch.setenv("NUMBER", "0")
    with pytest.raises(ValueError):
        readInteger("NUMBER", 1)


def test_database_builds_and_closes_pool(monkeypatch, serviceConfig):
    class Engine:
        def dispose(self):
            self.closed = True

    engine = Engine()
    captured = {}

    def createEngine(url, **values):
        captured.update(values)
        return engine

    def sessionMaker(**values):
        return object()

    monkeypatch.setattr("sentence_similarity.database.create_engine", createEngine)
    monkeypatch.setattr("sentence_similarity.database.sessionmaker", sessionMaker)
    database = Database(serviceConfig)
    database.close()
    assert captured["pool_pre_ping"] is True
    assert engine.closed is True


def test_opensearch_client_uses_aws_signing(monkeypatch, serviceConfig):
    credentials = object()

    class Session:
        def get_credentials(self):
            return credentials

    captured = {}

    def sessionFactory(**values):
        return Session()

    def signer(*values):
        return "auth"

    def opensearch(**values):
        captured.update(values)
        return "client"

    monkeypatch.setattr(
        "sentence_similarity.open_search_client.boto3.Session",
        sessionFactory,
    )
    monkeypatch.setattr(
        "sentence_similarity.open_search_client.AWSV4SignerAuth",
        signer,
    )
    monkeypatch.setattr(
        "sentence_similarity.open_search_client.OpenSearch",
        opensearch,
    )

    assert OpenSearchClient(serviceConfig).create_client() == "client"
    assert captured["pool_maxsize"] == 20
    assert captured["max_retries"] == 5


def test_opensearch_client_requires_credentials(monkeypatch, serviceConfig):
    class Session:
        def get_credentials(self):
            return None

    def sessionFactory(**values):
        return Session()

    monkeypatch.setattr(
        "sentence_similarity.open_search_client.boto3.Session",
        sessionFactory,
    )
    with pytest.raises(RuntimeError):
        OpenSearchClient(serviceConfig).create_client()


def test_opensearch_client_can_assume_role(monkeypatch, serviceConfig):
    serviceConfig.iamAccessRole = "arn:aws:iam::1:role/opensearch"

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

    assumedSession = object()

    def sessionFactory(**values):
        if "aws_access_key_id" in values:
            return assumedSession
        return BaseSession()

    monkeypatch.setattr(
        "sentence_similarity.open_search_client.boto3.Session",
        sessionFactory,
    )
    assert OpenSearchClient(serviceConfig)._session() is assumedSession
