class CaseSimilarityError(Exception):
    """Base error for the new Case Similarity flow."""


class PermanentDataError(CaseSimilarityError):
    """The message is valid JSON but required database data is missing."""


class OperationFailedError(CaseSimilarityError):
    """A retryable operation used all of its attempts."""


class QueueSendError(CaseSimilarityError):
    """The completed document could not be sent to the next queue."""


class KafkaPublishError(CaseSimilarityError):
    """A changed TAH status could not be published to Kafka."""
