import time
from collections.abc import Callable
from typing import Any

from botocore.exceptions import BotoCoreError
from sqlalchemy.exc import SQLAlchemyError

from case_similarity_completed.opensearch import constants
from case_similarity_completed.opensearch.errors import KafkaPublishError, OperationFailedError, QueueSendError
from ta_utils.logger.CustomLogger import CustomLogger

RETRYABLE_ERRORS = (BotoCoreError, SQLAlchemyError, TimeoutError, ConnectionError, KafkaPublishError, QueueSendError)


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
    """Retry one operation without repeating earlier completed work. Supported
    transient failures retry until the operation succeeds or reaches the limit.

    Input: label="Refresh Titan case summaries", operation=refresh_bidirectional_case_summaries,
    wait_seconds=5, max_attempts=3
    Output: the operation result without modification.

    max_attempts=3 means one initial execution and two retries.

    Raises:
        OperationFailedError: If all retry attempts fail.
    """

    for attempt in range(1, max_attempts + 1):
        try:
            result = operation(*arguments)
            if attempt > 1:
                custom_logger.info(f"{label} succeeded on attempt {attempt} after {attempt - 1} failed attempt(s).")
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
