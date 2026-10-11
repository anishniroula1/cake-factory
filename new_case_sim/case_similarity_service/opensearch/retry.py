import time
from collections.abc import Callable
from typing import Any

from botocore.exceptions import BotoCoreError
from sqlalchemy.exc import SQLAlchemyError

from case_similarity_service.opensearch.errors import KafkaPublishError, OperationFailedError, QueueSendError
from ta_utils.logger.CustomLogger import CustomLogger

RETRYABLE_ERRORS = (BotoCoreError, SQLAlchemyError, TimeoutError, ConnectionError, KafkaPublishError, QueueSendError)


def retry_operation[Result](
    label: str,
    operation: Callable[..., Result],
    *arguments: Any,
    wait_seconds: int,
    custom_logger: CustomLogger,
    max_attempts: int = 3,
) -> Result:
    """Retry one operation without repeating earlier completed work."""

    for attempt in range(1, max_attempts + 1):
        try:
            result = operation(*arguments)
            if attempt > 1:
                custom_logger.info(f"{label} succeeded on attempt {attempt} after {attempt - 1} failed attempt(s).")
            return result
        except RETRYABLE_ERRORS as error:
            if attempt == max_attempts:
                raise OperationFailedError(
                    f"{label} failed after {max_attempts} attempts: {str(error)[:500]}"
                ) from error
            custom_logger.warning(
                f"{label} attempt {attempt} failed: {str(error)[:500]}; retrying in {wait_seconds} seconds"
            )
            time.sleep(wait_seconds)

    raise OperationFailedError(f"{label} ended without returning a result")
