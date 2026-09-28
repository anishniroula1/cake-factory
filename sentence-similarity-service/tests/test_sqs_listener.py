import json

import pytest

from sentence_similarity.errors import PermanentMessageError
from sentence_similarity.sqs_listener import SqsListener
from tests.fakes import SqsFake


class Handler:
    def __init__(self, fails=False):
        self.fails = fails
        self.values = []

    def process(self, tspId):
        if self.fails:
            raise RuntimeError("failed")
        self.values.append(tspId)


def listener(serviceConfig, sqs=None, handler=None):
    return SqsListener(
        serviceConfig,
        sqs or SqsFake(),
        handler or Handler(),
    )


def test_tsp_id_parser_accepts_snake_and_camel_case(serviceConfig):
    worker = listener(serviceConfig)
    assert worker._tspId('{"tsp_id": 10}') == 10
    assert worker._tspId('{"tspId": "11"}') == 11


def test_tsp_id_parser_rejects_invalid_messages(serviceConfig):
    worker = listener(serviceConfig)
    for body in ["bad", "[]", "{}", '{"tsp_id": 0}']:
        with pytest.raises(PermanentMessageError):
            worker._tspId(body)


def test_successful_message_is_deleted(serviceConfig):
    sqs = SqsFake()
    handler = Handler()
    worker = listener(serviceConfig, sqs, handler)
    worker._processMessage(
        {
            "Body": json.dumps({"tsp_id": 10}),
            "ReceiptHandle": "receipt",
        }
    )
    assert handler.values == [10]
    assert sqs.deleted[0]["ReceiptHandle"] == "receipt"


def test_invalid_or_failed_message_is_not_deleted(serviceConfig):
    sqs = SqsFake()
    worker = listener(serviceConfig, sqs, Handler(fails=True))
    worker._processMessage({"Body": "bad", "ReceiptHandle": "one"})
    worker._processMessage(
        {"Body": json.dumps({"tsp_id": 10}), "ReceiptHandle": "two"}
    )
    assert sqs.deleted == []


def test_listener_keeps_polling(serviceConfig):
    class Queue(SqsFake):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def receive_message(self, **values):
            self.calls += 1
            if self.calls == 1:
                return {"Messages": []}
            raise KeyboardInterrupt

    queue = Queue()
    with pytest.raises(KeyboardInterrupt):
        listener(serviceConfig, queue).run()
    assert queue.calls == 2
