import time
from collections.abc import Callable
from typing import Any

from botocore.exceptions import BotoCoreError
from opensearchpy.exceptions import OpenSearchException
from sqlalchemy.exc import SQLAlchemyError

from similarity_completed.opensearch.errors import OperationFailedError, PermanentDataError, QueueSendError
from ta_utils.logger.CustomLogger import CustomLogger

RETRYABLE_ERRORS = (BotoCoreError, OpenSearchException, SQLAlchemyError, TimeoutError, ConnectionError, QueueSendError)


def _single_line(value: object, limit: int = 500) -> str:
    """Normalize a diagnostic value without including an exception traceback."""

    return " ".join(str(value).split())[:limit]


def safe_error_summary(error: object) -> str:
    """Return useful error details without SQL, parameters, or response bodies.

    SQLAlchemy includes the complete statement and bound parameters in its
    exception string. PostgreSQL exposes the actual failure separately through
    the driver's diagnostic object, so database summaries use only its primary
    message and detail.
    """

    error_name = type(error).__name__
    if getattr(error, "safe_for_logging", False):
        return f"{error_name}: {str(error)[:500]}"
    original_error = getattr(error, "orig", None)
    if original_error is not None:
        summary = f"{error_name} (database_error={type(original_error).__name__})"
        diagnostic = getattr(original_error, "diag", None)
        diagnostic_messages = []
        if diagnostic is not None:
            for attribute in ("message_primary", "message_detail"):
                value = getattr(diagnostic, attribute, None)
                if value:
                    diagnostic_messages.append(_single_line(value))
        if diagnostic_messages:
            summary = f"{summary}: {'; '.join(diagnostic_messages)}"
        return summary[:1000]
    return error_name


def retry_operation[Result](
    label: str,
    operation: Callable[..., Result],
    *arguments: Any,
    wait_seconds: int,
    custom_logger: CustomLogger,
    max_attempts: int = 3,
) -> Result:
    """Execute an operation with bounded retries for transient failures.
    Non-retryable data errors stop immediately; supported transient failures
    retry until the operation succeeds or reaches the configured maximum.

    Input: label="Save Titan match batch 1", operation=save_match_batch,
    wait_seconds=5, max_attempts=3
    Output: the operation result without modification. For example, an operation
    returning 500 makes this function return 500.

    max_attempts=3 means one initial execution and two retries. Positional
    arguments supplied after operation are forwarded directly to that callable.

    Raises:
        ValueError: If max_attempts is less than one.
        PermanentDataError: If the operation reports non-retryable data.
        OperationFailedError: If all retry attempts fail.
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
                ) from None
            custom_logger.warning(
                f"{label} attempt {attempt} failed: {error_summary}; retrying in {wait_seconds} seconds"
            )
            time.sleep(wait_seconds)

    raise OperationFailedError(f"{label} ended without returning a result")
