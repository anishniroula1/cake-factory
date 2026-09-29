from doc2vec_service.custom_logger import CustomLogger
from tests.fakes import LoggerFake


def test_custom_logger_forwards_every_message_type():
    backend = LoggerFake()
    logger = CustomLogger("test", logger=backend)

    logger.debug("debug")
    logger.info("info")
    logger.warning("warning")
    logger.error("error")
    logger.exception("exception")

    assert backend.debugMessages == ["debug"]
    assert backend.infoMessages == ["info"]
    assert backend.warningMessages == ["warning"]
    assert backend.errorMessages == ["error"]
    assert backend.exceptionMessages == ["exception"]
