from types import SimpleNamespace

import pytest

from doc2vec_service.errors import PermanentMessageError
from doc2vec_service.sqs_processor import SqsProcessor
from tests.fakes import SqsFake


class Handler:
    def __init__(self):
        self.values = []

    def process(self, value):
        self.values.append(value)


def processor(customLogger, sqs=None, handler=None):
    dependencies = SimpleNamespace(
        awsSqs=sqs or SqsFake(),
        historicalHandler=handler or Handler(),
        logger=customLogger,
    )
    return SqsProcessor(dependencies)


def historicalMessage(tsp_id=3):
    return {
        "reason": "HISTORICAL",
        "tsp_id": tsp_id,
        "MessageId": "message-1",
        "ReceiptHandle": "receipt-1",
        "message_receive_count": 1,
    }


def test_message_reader_keeps_string_or_number_tsp_id(customLogger):
    worker = processor(customLogger)
    assert worker._read_tsp_id({"tsp_id": "document-10"}) == "document-10"
    assert worker._read_tsp_id({"tsp_id": 11}) == 11


def test_message_reader_rejects_only_a_missing_tsp_id(
    customLogger,
):
    worker = processor(customLogger)
    with pytest.raises(PermanentMessageError):
        worker._read_tsp_id({"reason": "HISTORICAL"})
    with pytest.raises(PermanentMessageError):
        worker._read_tsp_id({"wrong_name": 12})


def test_successful_historical_message_is_processed_and_deleted(
    customLogger,
):
    sqs = SqsFake()
    handler = Handler()
    worker = processor(customLogger, sqs, handler)
    message = historicalMessage()

    worker._processMessage(message)

    assert handler.values == [3]
    assert sqs.deleted == [message]


def test_invalid_or_failed_message_is_not_deleted(customLogger):
    sqs = SqsFake()
    worker = processor(customLogger, sqs)
    invalidMessage = historicalMessage()
    invalidMessage.pop("tsp_id")
    worker._processMessage(invalidMessage)

    class FailingHandler:
        def process(self, value):
            raise RuntimeError("temporary failure")

    worker.dependencies.historicalHandler = FailingHandler()
    worker._processMessage(historicalMessage())
    assert sqs.deleted == []


def test_message_without_historical_reason_is_logged_and_not_processed(
    customLogger,
):
    sqs = SqsFake()
    handler = Handler()
    worker = processor(customLogger, sqs, handler)

    worker._processMessage({"tsp_id": 3, "ReceiptHandle": "one"})
    worker._processMessage(
        {"reason": "TRAIN", "tsp_id": 3, "ReceiptHandle": "two"}
    )

    assert handler.values == []
    assert sqs.deleted == []
    assert len(customLogger.warningMessages) == 2


def test_processor_keeps_receiving_messages(customLogger):
    class Queue(SqsFake):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def listen_sqs_message(self):
            self.calls += 1
            if self.calls == 1:
                return [historicalMessage()]
            raise KeyboardInterrupt

    queue = Queue()
    handler = Handler()
    worker = processor(customLogger, queue, handler)
    with pytest.raises(KeyboardInterrupt):
        worker.run()
    assert queue.calls == 2
    assert handler.values == [3]


def test_delete_failure_is_logged(customLogger):
    sqs = SqsFake()
    sqs.deleteResult = False
    worker = processor(customLogger, sqs)

    worker._processMessage(historicalMessage())

    assert len(customLogger.warningMessages) == 1
