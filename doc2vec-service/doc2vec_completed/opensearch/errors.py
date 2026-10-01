class PermanentMessageError(ValueError):
    """The SQS payload or source data cannot succeed without a correction."""


class PermanentDataError(ValueError):
    """A stored record is invalid and should not retry forever."""
