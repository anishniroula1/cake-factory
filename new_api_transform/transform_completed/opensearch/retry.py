import time
from collections.abc import Callable
from typing import Any

from botocore.exceptions import BotoCoreError
from confluent_kafka import KafkaException
from sqlalchemy.exc import SQLAlchemyError

from ta_utils.logger.CustomLogger import CustomLogger
from transform_completed.opensearch import constants
from transform_completed.opensearch.errors import KafkaPublishError, OperationFailedError

RETRYABLE_ERRORS = (BotoCoreError, SQLAlchemyError, KafkaException, KafkaPublishError, TimeoutError, ConnectionError)


def safe_error_summary(error: object) -> str:
    """Return useful error details without SQL, parameters, or response bodies."""

    error_name = type(error).__name__
    if getattr(error, "safe_for_logging", False):
        return f"{error_name}: {str(error)[:500]}"
    original_error = getattr(error, "orig", None)
    if original_error is not None:
        return f"{error_name} (database_error={type(original_error).__name__})"
    return error_name


def retry_operation[Result](
    label: str,
    operation: Callable[..., Result],
    *arguments: Any,
    wait_seconds: int,
    custom_logger: CustomLogger,
    max_attempts: int = constants.MAX_ATTEMPTS,
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
            error_summary = safe_error_summary(error)
            if attempt == max_attempts:
                raise OperationFailedError(
                    f"{label} failed after {max_attempts} attempts: {error_summary}"
                ) from error
            custom_logger.warning(
                f"{label} attempt {attempt} failed: {error_summary}; retrying in {wait_seconds} seconds"
            )
            time.sleep(wait_seconds)
    raise OperationFailedError(f"{label} ended without returning a result")
