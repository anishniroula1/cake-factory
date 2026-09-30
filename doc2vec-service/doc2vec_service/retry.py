import time
from collections.abc import Callable

from doc2vec_service.contracts import Logger
from doc2vec_service.errors import PermanentDataError, PermanentMessageError


def retryOperation[**Parameters, Result](
    label: str,
    operation: Callable[Parameters, Result],
    *arguments: Parameters.args,
    waitSeconds: int,
    maxAttempts: int = 3,
    customLogger: Logger | None = None,
) -> Result:
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
                if customLogger:
                    customLogger.error(
                        f"{label} failed after {maxAttempts} attempt(s): "
                        f"{str(error)[:500]}"
                    )
                raise
            if customLogger:
                customLogger.warning(
                    f"{label} attempt {attempt} failed: {str(error)[:500]}; "
                    f"retrying in {waitSeconds} seconds"
                )
            time.sleep(waitSeconds)
