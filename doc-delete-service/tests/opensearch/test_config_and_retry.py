from dataclasses import replace

import pytest
from sqlalchemy.exc import OperationalError

from doc_delete_completed.opensearch.config import Config, _aws_hostname
from doc_delete_completed.opensearch.errors import OperationFailedError, PermanentDataError
from doc_delete_completed.opensearch.retry import retry_operation
from tests.opensearch.fakes import LoggerFake


def config() -> Config:
    return Config(
        pg_db_user="database-user",
        pg_db_password="database-password",
        pg_db_host="database-host",
        pg_db_database="database-name",
        pg_db_port=5432,
        dynamic_credential_file="",
        environment="local",
        aws_region="us-east-1",
        aws_local_endpoint_url="",
        opensearch_iam_access_role="",
        opensearch_host="search.example.com",
        receive_sqs_endpoint="https://sqs.example/source-local.fifo",
        sent_sqs_endpoint="https://sqs.example/next-local.fifo",
        database_delete_enabled=True,
    )


def test_config_normalizes_host_and_validates_required_values():
    assert _aws_hostname("https://search.example.com") == "search.example.com"
    assert _aws_hostname("") == ""

    config().validate()
    with pytest.raises(ValueError, match="OPENSEARCH_HOST"):
        replace(config(), opensearch_host="").validate()
    with pytest.raises(ValueError, match="VAULT_YAML_PATH"):
        replace(config(), environment="prod").validate()
    with pytest.raises(ValueError, match="must be an ARN"):
        replace(config(), opensearch_iam_access_role="invalid").validate()


def test_retry_reports_success_and_exhaustion(monkeypatch):
    attempts = []

    def operation():
        attempts.append(1)
        if len(attempts) == 1:
            raise OperationalError("statement", {}, Exception("temporary"))
        return 10

    monkeypatch.setattr("doc_delete_completed.opensearch.retry.time.sleep", lambda seconds: None)
    logger = LoggerFake()
    assert retry_operation("operation", operation, wait_seconds=5, custom_logger=logger) == 10

    def always_fails():
        raise OperationalError("statement", {}, Exception("temporary"))

    with pytest.raises(OperationFailedError):
        retry_operation("operation", always_fails, wait_seconds=5, custom_logger=logger, max_attempts=2)
    with pytest.raises(PermanentDataError):
        retry_operation(
            "operation",
            lambda: (_ for _ in ()).throw(PermanentDataError("bad data")),
            wait_seconds=5,
            custom_logger=logger,
        )
    with pytest.raises(ValueError):
        retry_operation("operation", lambda: None, wait_seconds=5, custom_logger=logger, max_attempts=0)
