from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from doc2vec_completed.opensearch import open_search_client as client_module
from doc2vec_completed.opensearch.open_search_client import OpenSearchClient


class SessionFake:
    def __init__(self, credentials=None, sts_client=None):
        self.credentials = credentials
        self.sts_client = sts_client

    def get_credentials(self):
        return self.credentials

    def client(self, service_name):
        assert service_name == "sts"
        return self.sts_client


class StsClientFake:
    def __init__(self):
        self.arguments = None

    def assume_role(self, **arguments):
        self.arguments = arguments
        return {
            "Credentials": {
                "AccessKeyId": "access-key",
                "SecretAccessKey": "secret-key",
                "SessionToken": "session-token",
                "Expiration": datetime(2030, 1, 1, tzinfo=UTC),
            }
        }


def client_config(iam_role=""):
    return SimpleNamespace(
        aws_region="us-east-1",
        opensearch_service="es",
        opensearch_host="search.example.com",
        opensearch_port=443,
        opensearch_iam_access_role=iam_role,
    )


def test_create_client_uses_active_aws_credentials(monkeypatch):
    credentials = object()
    session = SessionFake(credentials=credentials)
    captured = {}

    monkeypatch.setattr(client_module.boto3, "Session", lambda **values: session)

    def build_auth(received_credentials, region, service):
        captured["auth"] = (received_credentials, region, service)
        return "signed-auth"

    def build_client(**arguments):
        captured["client"] = arguments
        return "opensearch-client"

    monkeypatch.setattr(client_module, "AWSV4SignerAuth", build_auth)
    monkeypatch.setattr(client_module, "OpenSearch", build_client)

    result = OpenSearchClient(client_config()).create_client()

    assert result == "opensearch-client"
    assert captured["auth"] == (credentials, "us-east-1", "es")
    assert captured["client"]["hosts"] == [{"host": "search.example.com", "port": 443}]
    assert captured["client"]["http_auth"] == "signed-auth"
    assert captured["client"]["use_ssl"] is True


def test_create_client_requires_aws_credentials(monkeypatch):
    client = OpenSearchClient(client_config())
    monkeypatch.setattr(client, "_session", lambda: SessionFake())

    with pytest.raises(RuntimeError, match="AWS credentials are required"):
        client.create_client()


def test_client_can_assume_the_configured_role(monkeypatch):
    sts_client = StsClientFake()
    base_session = SessionFake(sts_client=sts_client)
    assumed_session = SessionFake(credentials=object())
    session_calls = []

    def build_session(**arguments):
        session_calls.append(arguments)
        if len(session_calls) == 1:
            return base_session
        return assumed_session

    monkeypatch.setattr(client_module.boto3, "Session", build_session)

    result = OpenSearchClient(
        client_config("arn:aws:iam::123456789012:role/opensearch")
    )._session()

    assert result is assumed_session
    assert sts_client.arguments == {
        "RoleArn": "arn:aws:iam::123456789012:role/opensearch",
        "RoleSessionName": "doc2vecSentenceIngestion",
    }
    assert list(session_calls[1]) == ["botocore_session"]
