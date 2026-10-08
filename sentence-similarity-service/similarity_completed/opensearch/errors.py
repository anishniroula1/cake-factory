class CriticalProcessingError(RuntimeError):
    """Stop the current message when processing cannot safely continue."""


class PermanentDataError(CriticalProcessingError):
    """Report data that retrying cannot repair."""


class OperationFailedError(CriticalProcessingError):
    """Report an external operation that exhausted its retries."""


class QueueSendError(CriticalProcessingError):
    """Report that the completed document could not reach the next queue."""
