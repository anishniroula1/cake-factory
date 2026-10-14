import time
from collections.abc import Callable
from typing import Any

from botocore.exceptions import BotoCoreError
from confluent_kafka import KafkaException
from sqlalchemy.exc import SQLAlchemyError

from api_transform_service.opensearch.errors import KafkaPublishError, OperationFailedError
from ta_utils.logger.CustomLogger import CustomLogger

RETRYABLE_ERRORS = (BotoCoreError, SQLAlchemyError, KafkaException, KafkaPublishError, TimeoutError, ConnectionError)


def retry_operation[Result](
    label: str,
    operation: Callable[..., Result],
    *arguments: Any,
    wait_seconds: int,
    custom_logger: CustomLogger,
    max_attempts: int = 3,
) -> Result:
    """Retry only the failed operation and report when a later attempt succeeds.

    Input: max_attempts=3 runs the operation no more than three times.
    Output: the operation result from its first successful attempt.

    Raises: OperationFailedError after all configured attempts fail.
    """

    for attempt in range(1, max_attempts + 1):
        try:
            result = operation(*arguments)
            if attempt > 1:
                custom_logger.info(f"{label} succeeded on attempt {attempt} after {attempt - 1} failure(s).")
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
