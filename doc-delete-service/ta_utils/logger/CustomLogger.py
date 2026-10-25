import logging


class CustomLogger:
    """Expose the logging methods supplied by the shared production library."""

    def __init__(self, name: str) -> None:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        self.logger = logging.getLogger(name)
        self.generic_message: str | None = None

    def set_generic_message(self, message: str | None) -> None:
        """Set or clear the tsp_id context included by the production logger."""

        self.generic_message = message

    def debug(self, message: str) -> None:
        self.logger.debug(message)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

