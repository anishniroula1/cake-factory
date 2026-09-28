import pytest

from sentence_similarity.errors import PermanentDataError
from sentence_similarity.retry import retryOperation


def noWait(value):
    pass


def test_retry_returns_after_temporary_failure(monkeypatch):
    attempts = []

    def operation(value):
        attempts.append(value)
        if len(attempts) == 1:
            raise RuntimeError("temporary")
        return value

    monkeypatch.setattr("sentence_similarity.retry.time.sleep", noWait)
    assert retryOperation("work", operation, "done", waitSeconds=5) == "done"
    assert len(attempts) == 2


def test_retry_stops_at_limit_and_rejects_invalid_limit(monkeypatch):
    attempts = []

    def operation():
        attempts.append(1)
        raise RuntimeError("unavailable")

    monkeypatch.setattr("sentence_similarity.retry.time.sleep", noWait)
    with pytest.raises(RuntimeError):
        retryOperation("work", operation, waitSeconds=5, maxAttempts=2)
    assert len(attempts) == 2
    with pytest.raises(ValueError):
        retryOperation("work", operation, waitSeconds=5, maxAttempts=0)


def test_retry_does_not_hide_permanent_data_error():
    def operation():
        raise PermanentDataError("bad data")

    with pytest.raises(PermanentDataError):
        retryOperation("work", operation, waitSeconds=5)
