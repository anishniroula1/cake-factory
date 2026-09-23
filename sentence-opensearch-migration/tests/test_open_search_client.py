from types import SimpleNamespace

from sentence_migration import open_search_client
from sentence_migration.open_search_client import OpenSearchClient


def test_opensearch_client_assumes_selected_environment_role(monkeypatch):
    calls = []
    final_session = object()

    class StsClient:
        def assume_role(self, **kwargs):
            calls.append(kwargs)
            return {
                "Credentials": {
                    "AccessKeyId": "access",
                    "SecretAccessKey": "secret",
                    "SessionToken": "token",
                }
            }

    class BaseSession:
        def client(self, service):
            assert service == "sts"
            return StsClient()

    def session_factory(**kwargs):
        if "aws_access_key_id" in kwargs:
            assert kwargs["aws_access_key_id"] == "access"
            assert kwargs["aws_secret_access_key"] == "secret"
            assert kwargs["aws_session_token"] == "token"
            return final_session
        assert kwargs == {"region_name": "us-east-1"}
        return BaseSession()

    monkeypatch.setattr(open_search_client.boto3, "Session", session_factory)
    config = SimpleNamespace(
        aws_region="us-east-1",
        aws_role_external_id="external-id",
    )
    role = "arn:aws:iam::123456789012:role/approved-role"

    session = OpenSearchClient(config, role)._session()

    assert session is final_session
    assert calls == [
        {
            "RoleArn": role,
            "RoleSessionName": "sentenceOpenSearchMigrationSession",
            "ExternalId": "external-id",
        }
    ]
