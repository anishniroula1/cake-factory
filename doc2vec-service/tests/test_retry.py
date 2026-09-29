import pytest

from doc2vec_service.errors import PermanentDataError
from doc2vec_service.retry import retryOperation


def noWait(value):
    pass


def test_retry_operation_retries_then_returns(monkeypatch, customLogger):
    attempts = []

    def operation():
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("temporary")
        return "done"

    monkeypatch.setattr("doc2vec_service.retry.time.sleep", noWait)
    assert retryOperation(
        "test",
        operation,
        waitSeconds=5,
        customLogger=customLogger,
    ) == "done"
    assert len(attempts) == 2
    assert len(customLogger.warningMessages) == 1


def test_retry_operation_does_not_hide_permanent_data_error():
    def permanentFailure():
        raise PermanentDataError("bad data")

    with pytest.raises(PermanentDataError):
        retryOperation("test", permanentFailure, waitSeconds=5)


def test_retry_operation_stops_at_passed_attempt_limit(
    monkeypatch,
    customLogger,
):
    attempts = []

    def operation():
        attempts.append(1)
        raise RuntimeError("still unavailable")

    monkeypatch.setattr("doc2vec_service.retry.time.sleep", noWait)
    with pytest.raises(RuntimeError):
        retryOperation(
            "test",
            operation,
            waitSeconds=5,
            maxAttempts=2,
            customLogger=customLogger,
        )
    assert len(attempts) == 2
    assert len(customLogger.errorMessages) == 1


def test_retry_operation_rejects_invalid_attempt_limit():
    with pytest.raises(ValueError):
        retryOperation("test", noWait, waitSeconds=5, maxAttempts=0)
