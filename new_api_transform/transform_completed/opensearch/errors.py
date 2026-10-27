class ApiTransformError(Exception):
    """Base error for the new API Transform flow."""

    safe_for_logging = True


class PermanentDataError(ApiTransformError):
    """Required data is missing, so retrying the same data will not help."""


class OperationFailedError(ApiTransformError):
    """A retryable operation used all of its attempts."""


class KafkaPublishError(ApiTransformError):
    """Kafka did not confirm every application event."""
