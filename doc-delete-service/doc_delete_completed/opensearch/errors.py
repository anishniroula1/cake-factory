class PermanentDataError(Exception):
    """The message cannot be processed because required source data is invalid."""


class OperationFailedError(Exception):
    """A retryable operation exhausted its configured attempts."""


class QueueSendError(Exception):
    """The next service message could not be sent."""

