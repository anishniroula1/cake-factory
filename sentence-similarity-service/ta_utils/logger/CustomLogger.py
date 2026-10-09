import logging


class CustomLogger:
    def __init__(self, name: str) -> None:
        """Create the default logger until the team logger is supplied."""

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        self.logger = logging.getLogger(name)

    def debug(self, message: str) -> None:
        self.logger.debug(message)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

    def set_generic_message(self, message: str | None) -> None:
        """Set shared log context when the team logger is available."""
