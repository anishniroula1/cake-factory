import time
from collections.abc import Callable

from doc2vec_completed.opensearch.custom_logger import CustomLogger
from doc2vec_completed.opensearch.errors import (
    PermanentDataError,
    PermanentMessageError,
)


def retry_operation[**Parameters, Result](
    label: str,
    operation: Callable[Parameters, Result],
    *arguments: Parameters.args,
    wait_seconds: int,
    max_attempts: int = 3,
    custom_logger: CustomLogger | None = None,
) -> Result:
    """Try an operation up to three times unless another limit is passed.

    Input: max_attempts=5 allows up to five total attempts. Extra values are
    passed directly to the operation.
    """

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    for attempt in range(1, max_attempts + 1):
        try:
            return operation(*arguments)
        except Exception as error:
            if isinstance(error, (PermanentDataError, PermanentMessageError)):
                raise
            if attempt == max_attempts:
                if custom_logger:
                    custom_logger.error(
                        f"{label} failed after {max_attempts} attempt(s): "
                        f"{str(error)[:500]}"
                    )
                raise
            if custom_logger:
                custom_logger.warning(
                    f"{label} attempt {attempt} failed: {str(error)[:500]}; "
                    f"retrying in {wait_seconds} seconds"
                )
            time.sleep(wait_seconds)
