class PermanentDataError(RuntimeError):
    """Report data that retrying cannot repair."""


class PermanentMessageError(RuntimeError):
    """Report an invalid SQS message."""
