from dataclasses import replace

import pytest

from doc2vec_service.config import Config, readInteger
from doc2vec_service.open_search_client import OpenSearchClient


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


def test_opensearch_client_uses_aws_signing(monkeypatch, serviceConfig):
    serviceConfig.awsRegion = "us-east-1"
    serviceConfig.iamAccessRole = ""
    serviceConfig.opensearchService = "es"
    serviceConfig.opensearchHost = "search.example.com"
    serviceConfig.opensearchPort = 443
    credentials = object()

    class Session:
        def get_credentials(self):
            return credentials

    captured = {}

    def signer(*values):
        return "auth"

    def opensearch(**values):
        captured.update(values)
        return "client"

    monkeypatch.setattr(
        "doc2vec_service.open_search_client.AWSV4SignerAuth",
        signer,
    )
    monkeypatch.setattr(
        "doc2vec_service.open_search_client.OpenSearch",
        opensearch,
    )

    def activeSession(**values):
        return Session()

    monkeypatch.setattr(
        "doc2vec_service.open_search_client.boto3.Session",
        activeSession,
    )
    client = OpenSearchClient(serviceConfig)
    assert client.create_client() == "client"
    assert captured["http_auth"] == "auth"
    assert captured["pool_maxsize"] == 20
    assert captured["max_retries"] == 5

    class EmptySession:
        def get_credentials(self):
            return None

    def emptySession(**values):
        return EmptySession()

    monkeypatch.setattr(
        "doc2vec_service.open_search_client.boto3.Session",
        emptySession,
    )
    with pytest.raises(RuntimeError):
        OpenSearchClient(serviceConfig).create_client()


def test_opensearch_client_assumes_configured_role(monkeypatch, serviceConfig):
    serviceConfig.awsRegion = "us-east-1"
    serviceConfig.iamAccessRole = "arn:aws:iam::1:role/opensearch"

    class Sts:
        def assume_role(self, **values):
            assert values["RoleArn"] == serviceConfig.iamAccessRole
            return {
                "Credentials": {
                    "AccessKeyId": "key",
                    "SecretAccessKey": "secret",
                    "SessionToken": "token",
                }
            }

    class BaseSession:
        def client(self, name):
            assert name == "sts"
            return Sts()

    assumedSession = object()
    calls = []

    def sessionFactory(**values):
        calls.append(values)
        if "aws_access_key_id" in values:
            return assumedSession
        return BaseSession()

    monkeypatch.setattr(
        "doc2vec_service.open_search_client.boto3.Session",
        sessionFactory,
    )

    assert OpenSearchClient(serviceConfig)._session() is assumedSession
    assert calls[-1]["aws_session_token"] == "token"
