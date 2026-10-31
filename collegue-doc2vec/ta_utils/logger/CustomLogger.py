import logging


class CustomLogger:
    """Expose the logging interface supplied by the shared production library."""

    def __init__(self, name: str = "doc2vec_completed.opensearch", logger=None) -> None:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        self.logger = logger or logging.getLogger(name)
        self.generic_message: str | None = None

    def set_generic_message(self, message: str | None) -> None:
        """Set shared request context, such as tsp_id=1040, or clear it with None."""

        self.generic_message = message

    def debug(self, message: str) -> None:
        """Write a debug-level diagnostic message."""

        self.logger.debug(message)

    def info(self, message: str) -> None:
        """Write an informational service event."""

        self.logger.info(message)

    def warning(self, message: str) -> None:
        """Write a warning for a recoverable problem."""

        self.logger.warning(message)

    def error(self, message: str) -> None:
        """Write an error-level service event."""

        self.logger.error(message)

