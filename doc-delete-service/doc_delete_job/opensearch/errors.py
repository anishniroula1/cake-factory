class DocumentDeleteError(Exception):
    """Base error whose message is safe to include in service logs."""

    safe_for_logging = True


class PermanentDataError(DocumentDeleteError):
    """The message cannot be processed because required source data is invalid."""


class OperationFailedError(DocumentDeleteError):
    """A retryable operation exhausted its configured attempts."""


class QueueSendError(DocumentDeleteError):
    """The next service message could not be sent."""

