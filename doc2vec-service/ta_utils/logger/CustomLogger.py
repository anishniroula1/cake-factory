import logging


class CustomLogger:
    def __init__(self, name: str = "doc2vec_completed.opensearch", logger=None) -> None:
        """Use the supplied logger, or create a basic local logger."""

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        self.logger = logger or logging.getLogger(name)
        self.generic_message: str | None = None

    def set_generic_message(self, message: str | None) -> None:
        """Save shared log text such as tsp_id=1040."""

        self.generic_message = message

    def debug(self, message: str) -> None:
        """Write a message shown only when debug logging is enabled."""

        self.logger.debug(message)

    def info(self, message: str) -> None:
        """Write a normal progress message."""

        self.logger.info(message)

    def warning(self, message: str) -> None:
        """Write a warning for a problem that may recover."""

        self.logger.warning(message)

    def error(self, message: str) -> None:
        """Write an error for work that did not finish."""

        self.logger.error(message)
