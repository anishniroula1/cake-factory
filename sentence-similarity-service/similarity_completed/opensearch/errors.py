class CriticalProcessingError(RuntimeError):
    """Stop the current message when processing cannot safely continue."""

    safe_for_logging = True


class PermanentDataError(CriticalProcessingError):
    """Report data that retrying cannot repair."""


class OperationFailedError(CriticalProcessingError):
    """Report an external operation that exhausted its retries."""


class CatalogNotReadyError(TimeoutError):
    """Report catalog records that OpenSearch has not made visible yet."""

    safe_for_logging = True


class QueueSendError(CriticalProcessingError):
    """Report that the completed document could not reach the next queue."""
