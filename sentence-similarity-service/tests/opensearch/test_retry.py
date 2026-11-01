import pytest
from sqlalchemy.exc import IntegrityError

from similarity_completed.opensearch.errors import OperationFailedError, PermanentDataError
from similarity_completed.opensearch.retry import retry_operation, safe_error_summary
from tests.opensearch.fakes import LoggerFake


class PostgresDiagnostic:
    message_primary = 'insert or update on table "titan_matches" violates foreign key constraint'
    message_detail = 'Key (global_id)=(999) is not present in table "sentences_vector".'


class ForeignKeyViolation(Exception):
    diag = PostgresDiagnostic()


def test_retry_returns_after_temporary_failure(monkeypatch):
    attempts = []

    def operation(value):
        attempts.append(value)
        if len(attempts) == 1:
            raise TimeoutError("temporary")
        return value

    monkeypatch.setattr("similarity_completed.opensearch.retry.time.sleep", lambda value: None)
    logger = LoggerFake()
    result = retry_operation("work", operation, "done", wait_seconds=5, custom_logger=logger)

    assert result == "done"
    assert any(level == "info" and "succeeded on attempt 2" in message for level, message in logger.messages)
    assert "temporary" not in str(logger.messages)


def test_retry_stops_at_limit_and_rejects_bad_limit(monkeypatch):
    monkeypatch.setattr("similarity_completed.opensearch.retry.time.sleep", lambda value: None)
    logger = LoggerFake()

    def operation():
        raise TimeoutError("unavailable")

    with pytest.raises(OperationFailedError):
        retry_operation("work", operation, wait_seconds=5, max_attempts=2, custom_logger=logger)
    with pytest.raises(ValueError):
        retry_operation("work", operation, wait_seconds=5, max_attempts=0, custom_logger=logger)
    assert any(level == "warning" for level, message in logger.messages)


def test_permanent_data_error_is_not_retried():
    def operation():
        raise PermanentDataError("bad data")

    with pytest.raises(PermanentDataError):
        retry_operation("work", operation, wait_seconds=5, custom_logger=LoggerFake())


def test_database_error_summary_keeps_cause_without_sql_or_parameters():
    error = IntegrityError(
        "SELECT tsp.upsert_bidirectional_titan_matches(:matches)",
        {"matches": {"999": {"private": "large-input"}}},
        ForeignKeyViolation("foreign key violation"),
    )

    summary = safe_error_summary(error)

    assert "ForeignKeyViolation" in summary
    assert "Key (global_id)=(999) is not present" in summary
    assert "SELECT" not in summary
    assert "large-input" not in summary
