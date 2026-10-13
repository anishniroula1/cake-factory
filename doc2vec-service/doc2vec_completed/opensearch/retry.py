import time
from collections.abc import Callable
from typing import Any

from botocore.exceptions import BotoCoreError
from opensearchpy.exceptions import OpenSearchException
from sqlalchemy.exc import SQLAlchemyError

from doc2vec_completed.opensearch.errors import (
    OperationFailedError,
    PermanentDataError,
    QueueSendError,
)
from ta_utils.logger.CustomLogger import CustomLogger

RETRYABLE_ERRORS = (
    BotoCoreError,
    OpenSearchException,
    SQLAlchemyError,
    TimeoutError,
    ConnectionError,
    QueueSendError,
)


def retry_operation[Result](
    label: str,
    operation: Callable[..., Result],
    *arguments: Any,
    wait_seconds: int,
    max_attempts: int = 3,
    custom_logger: CustomLogger,
) -> Result:
    """Run one job again when an outside service has a temporary problem.

    Input: a label, the job to call, its values, wait time, and attempt limit.
    Output: the job result after it succeeds.

    Database, AWS, network, and OpenSearch errors may work on another try.
    Bad data and code bugs stop immediately because waiting will not fix them.
    """

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    for attempt in range(1, max_attempts + 1):
        try:
            result = operation(*arguments)
            if attempt > 1:
                custom_logger.info(
                    f"{label} succeeded on attempt {attempt} after "
                    f"{attempt - 1} failed attempt(s)."
                )
            return result
        except PermanentDataError:
            raise
        except RETRYABLE_ERRORS as error:
            if attempt == max_attempts:
                raise OperationFailedError(
                    f"{label} failed after {max_attempts} attempts: "
                    f"{str(error)[:500]}"
                ) from error
            custom_logger.warning(
                f"{label} attempt {attempt} failed: {str(error)[:500]}; "
                f"retrying in {wait_seconds} seconds"
            )
            time.sleep(wait_seconds)

    raise OperationFailedError(f"{label} ended without returning a result")
