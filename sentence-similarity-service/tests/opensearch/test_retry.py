import pytest

from similarity_completed.opensearch.errors import OperationFailedError, PermanentDataError
from similarity_completed.opensearch.retry import retry_operation
from tests.opensearch.fakes import LoggerFake


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
    assert len(attempts) == 2
    assert any(level == "info" and "succeeded on attempt 2" in message for level, message in logger.messages)


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
