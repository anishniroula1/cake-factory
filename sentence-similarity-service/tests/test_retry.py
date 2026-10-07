import pytest

from sentence_similarity.errors import PermanentDataError
from sentence_similarity.retry import retry_operation
from tests.fakes import LoggerFake


def test_retry_returns_after_temporary_failure(monkeypatch):
    attempts = []

    def operation(value):
        attempts.append(value)
        if len(attempts) == 1:
            raise RuntimeError("temporary")
        return value

    monkeypatch.setattr("sentence_similarity.retry.time.sleep", lambda value: None)
    result = retry_operation(
        "work",
        operation,
        "done",
        wait_seconds=5,
        custom_logger=LoggerFake(),
    )

    assert result == "done"
    assert len(attempts) == 2


def test_retry_stops_at_limit_and_rejects_bad_limit(monkeypatch):
    monkeypatch.setattr("sentence_similarity.retry.time.sleep", lambda value: None)
    logger = LoggerFake()

    def operation():
        raise RuntimeError("unavailable")

    with pytest.raises(RuntimeError):
        retry_operation(
            "work",
            operation,
            wait_seconds=5,
            max_attempts=2,
            custom_logger=logger,
        )
    with pytest.raises(ValueError):
        retry_operation("work", operation, wait_seconds=5, max_attempts=0)
    assert any(level == "error" for level, message in logger.messages)


def test_permanent_data_error_is_not_retried():
    def operation():
        raise PermanentDataError("bad data")

    with pytest.raises(PermanentDataError):
        retry_operation(
            "work",
            operation,
            wait_seconds=5,
            custom_logger=LoggerFake(),
        )
