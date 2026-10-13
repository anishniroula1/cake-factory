class CriticalProcessingError(RuntimeError):
    """Stop the current SQS message because its work cannot finish safely."""


class PermanentDataError(CriticalProcessingError):
    """Report bad data or a bad request that waiting will not fix."""


class OperationFailedError(CriticalProcessingError):
    """Report an outside call that still failed after every retry."""


class QueueSendError(CriticalProcessingError):
    """Report that the next service did not receive the completed document."""
