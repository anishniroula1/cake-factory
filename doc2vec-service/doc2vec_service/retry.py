import logging
import time
from collections.abc import Callable
from typing import Any

from doc2vec_service.errors import PermanentDataError, PermanentMessageError

logger = logging.getLogger(__name__)


def retryOperation(
    label: str,
    operation: Callable[..., Any],
    *arguments: Any,
    waitSeconds: int,
    maxAttempts: int = 3,
) -> Any:
    """Try an operation up to three times unless another limit is passed.

    Input: maxAttempts=5 allows up to five total attempts. Extra values are
    passed directly to the operation.
    """

    if maxAttempts < 1:
        raise ValueError("maxAttempts must be at least 1")

    for attempt in range(1, maxAttempts + 1):
        try:
            return operation(*arguments)
        except Exception as error:
            if isinstance(error, (PermanentDataError, PermanentMessageError)):
                raise
            if attempt == maxAttempts:
                logger.error(
                    "%s failed after %s attempt(s): %s",
                    label,
                    maxAttempts,
                    str(error)[:500],
                )
                raise
            logger.warning(
                "%s attempt %s failed: %s; retrying in %s seconds",
                label,
                attempt,
                str(error)[:500],
                waitSeconds,
            )
            time.sleep(waitSeconds)
