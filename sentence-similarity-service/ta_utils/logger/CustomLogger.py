import logging


class CustomLogger:
    """Expose the logging interface supplied by the shared production library."""

    def __init__(self, name: str) -> None:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        self.logger = logging.getLogger(name)

    def debug(self, message: str) -> None:
        """Write a debug-level diagnostic.

        Input: message="Executing the OpenSearch catalog query."
        """

        self.logger.debug(message)

    def info(self, message: str) -> None:
        """Write an informational service event.

        Input: message="Completed source sentence batch 1."
        """

        self.logger.info(message)

    def warning(self, message: str) -> None:
        """Write a recoverable warning.

        Input: message="OpenSearch request failed; retrying in 5 seconds."
        """

        self.logger.warning(message)

    def error(self, message: str) -> None:
        """Write an error-level service event.

        Input: message="OpenSearch request failed after 3 attempts."
        """

        self.logger.error(message)

    def set_generic_message(self, message: str | None) -> None:
        """Set request context when supported by the production logger.

        Input: message="tsp_id=8205", or message=None to clear the context.
        """
