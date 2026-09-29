import json

import pytest
from botocore.exceptions import ClientError

from doc2vec_service.errors import PermanentMessageError
from doc2vec_service.sqs_listener import SqsListener
from tests.fakes import SqsFake


class Handler:
    def __init__(self):
        self.values = []

    def process(self, value):
        self.values.append(value)


def listener(serviceConfig, customLogger, sqs=None, handler=None):
    return SqsListener(
        serviceConfig,
        sqs or SqsFake(),
        handler or Handler(),
        customLogger,
    )


def test_message_parser_reads_historical_tsp_id(serviceConfig, customLogger):
    worker = listener(serviceConfig, customLogger)
    assert worker._parseMessage(
        '{"reason":"HISTORICAL","tsp_id":"10"}'
    ) == {"tspId": 10}
    assert worker._parseMessage('{"tspId":11}') == {"tspId": 11}


def test_message_parser_rejects_bad_payloads(serviceConfig, customLogger):
    worker = listener(serviceConfig, customLogger)
    for body in ["not json", "[]", '{"reason":"HISTORICAL"}']:
        with pytest.raises(PermanentMessageError):
            worker._parseMessage(body)
    with pytest.raises(PermanentMessageError):
        worker._parseMessage('{"tsp_id":"bad"}')


def test_successful_historical_message_is_processed_and_deleted(
    serviceConfig,
    customLogger,
):
    sqs = SqsFake()
    handler = Handler()
    worker = listener(serviceConfig, customLogger, sqs, handler)
    worker._processMessage(
        {
            "Body": json.dumps({"reason": "HISTORICAL", "tsp_id": 3}),
            "ReceiptHandle": "receipt",
        }
    )

    assert handler.values == [3]
    assert sqs.deleted[0]["ReceiptHandle"] == "receipt"


def test_invalid_or_failed_message_is_not_deleted(serviceConfig, customLogger):
    sqs = SqsFake()
    worker = listener(serviceConfig, customLogger, sqs)
    worker._processMessage({"Body": "bad", "ReceiptHandle": "one"})

    class FailingHandler:
        def process(self, value):
            raise RuntimeError("temporary failure")

    worker.historicalHandler = FailingHandler()
    worker._processMessage(
        {
            "Body": json.dumps({"reason": "HISTORICAL", "tsp_id": 3}),
            "ReceiptHandle": "two",
        }
    )
    assert sqs.deleted == []


def test_listener_keeps_receiving_messages(serviceConfig, customLogger):
    class Queue(SqsFake):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def receive_message(self, **values):
            self.calls += 1
            if self.calls == 1:
                return {
                    "Messages": [
                        {
                            "Body": json.dumps({"tsp_id": 3}),
                            "ReceiptHandle": "receipt",
                        }
                    ]
                }
            raise KeyboardInterrupt

    queue = Queue()
    handler = Handler()
    worker = listener(serviceConfig, customLogger, queue, handler)
    with pytest.raises(KeyboardInterrupt):
        worker.run()
    assert queue.calls == 2
    assert handler.values == [3]


def test_listener_retries_an_sqs_receive_failure(
    serviceConfig,
    customLogger,
    monkeypatch,
):
    class Queue(SqsFake):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def receive_message(self, **values):
            self.calls += 1
            if self.calls == 1:
                raise ClientError(
                    {"Error": {"Code": "500", "Message": "failed"}},
                    "ReceiveMessage",
                )
            raise KeyboardInterrupt

    queue = Queue()
    monkeypatch.setattr("doc2vec_service.sqs_listener.time.sleep", lambda value: None)
    with pytest.raises(KeyboardInterrupt):
        listener(serviceConfig, customLogger, queue).run()
    assert queue.calls == 2


def test_sqs_delete_failure_does_not_reprocess_inside_the_listener(
    serviceConfig,
    customLogger,
):
    class Queue(SqsFake):
        def delete_message(self, **values):
            raise ClientError(
                {"Error": {"Code": "500", "Message": "failed"}},
                "DeleteMessage",
            )

    worker = listener(serviceConfig, customLogger, Queue())
    worker._processMessage(
        {
            "Body": json.dumps({"tsp_id": 3}),
            "ReceiptHandle": "receipt",
        }
    )
