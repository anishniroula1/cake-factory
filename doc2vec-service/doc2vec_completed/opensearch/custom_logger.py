import logging


class CustomLogger:
    def __init__(self, name: str = "doc2vec_completed.opensearch", logger=None) -> None:
        """Create the default logger used until the team logger is supplied."""

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        self.logger = logger or logging.getLogger(name)

    def debug(self, message: str) -> None:
        """Write a debug message."""

        self.logger.debug(message)

    def info(self, message: str) -> None:
        """Write an informational message."""

        self.logger.info(message)

    def warning(self, message: str) -> None:
        """Write a warning message."""

        self.logger.warning(message)

    def error(self, message: str) -> None:
        """Write an error message."""

        self.logger.error(message)

    def exception(self, message: str) -> None:
        """Write an error message with the current exception details."""

        self.logger.exception(message)
