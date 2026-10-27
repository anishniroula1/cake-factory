import time
from collections.abc import Callable
from typing import Any

from botocore.exceptions import BotoCoreError
from opensearchpy.exceptions import OpenSearchException
from sqlalchemy.exc import SQLAlchemyError

from doc_delete_completed.opensearch.errors import OperationFailedError, PermanentDataError, QueueSendError
from ta_utils.logger.CustomLogger import CustomLogger

RETRYABLE_ERRORS = (BotoCoreError, OpenSearchException, SQLAlchemyError, TimeoutError, ConnectionError, QueueSendError)


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
    max_attempts: int = 3,
) -> Result:
    """Run one operation again when a supported temporary failure occurs.

    Input: label="Delete sentence occurrences", max_attempts=3
    Output: the value returned by operation after one successful attempt.

    Three attempts means the first call plus two retries. Invalid source data
    stops immediately because repeating it cannot repair the message.
    """

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    for attempt in range(1, max_attempts + 1):
        try:
            result = operation(*arguments)
            if attempt > 1:
                custom_logger.info(f"{label} succeeded on attempt {attempt} after {attempt - 1} failed attempt(s).")
            return result
        except PermanentDataError:
            raise
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
