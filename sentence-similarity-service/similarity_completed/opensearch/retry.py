import time
from collections.abc import Callable
from typing import Any

from similarity_completed.opensearch.errors import PermanentDataError, PermanentMessageError
from ta_utils.logger.CustomLogger import CustomLogger


def retry_operation[Result](
    label: str,
    operation: Callable[..., Result],
    *arguments: Any,
    wait_seconds: int,
    max_attempts: int = 3,
    custom_logger: CustomLogger | None = None,
) -> Result:
    """Retry one complete operation up to the requested attempt limit."""

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    for attempt in range(1, max_attempts + 1):
        try:
            return operation(*arguments)
        except Exception as error:
            if isinstance(error, (PermanentDataError, PermanentMessageError)):
                if custom_logger:
                    custom_logger.error(f"{label} cannot be retried: {str(error)[:500]}")
                raise
            if attempt == max_attempts:
                if custom_logger:
                    custom_logger.error(f"{label} failed after {max_attempts} attempts: {str(error)[:500]}")
                raise
            if custom_logger:
                custom_logger.warning(
                    f"{label} attempt {attempt} failed: {str(error)[:500]}; retrying in {wait_seconds} seconds"
                )
            time.sleep(wait_seconds)

    raise RuntimeError("retry loop ended without returning or raising")
