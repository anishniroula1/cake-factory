import json

import pytest

from doc2vec_service.errors import PermanentMessageError
from doc2vec_service.sqs_listener import SqsListener
from tests.fakes import SqsFake


class Handler:
    def __init__(self):
        self.values = []

    def process(self, value=None):
        self.values.append(value)


def listener(serviceConfig, sqs=None):
    return SqsListener(
        serviceConfig,
        sqs or SqsFake(),
        Handler(),
        Handler(),
        Handler(),
    )


def test_message_parser_accepts_all_supported_names(serviceConfig):
    worker = listener(serviceConfig)
    assert worker._parseMessage('{"reason":"TRAIN_MODEL","tsp_id":"10"}') == {
        "reason": "TRAIN",
        "tspId": 10,
    }
    assert worker._parseMessage('{"reason":"TRACER"}') == {
        "reason": "TRACER",
        "tspId": None,
    }
    assert worker._parseMessage('{"tspId":11}')["reason"] == "LIVE"


def test_message_parser_rejects_bad_payloads(serviceConfig):
    worker = listener(serviceConfig)
    for body in ["not json", "[]", '{"reason":"OTHER"}', '{"reason":"TRAIN"}']:
        with pytest.raises(PermanentMessageError):
            worker._parseMessage(body)
    with pytest.raises(PermanentMessageError):
        worker._parseMessage('{"reason":"LIVE","tsp_id":"bad"}')


def test_unknown_reason_uses_live_flow_when_tsp_id_exists(serviceConfig):
    worker = listener(serviceConfig)
    message = worker._parseMessage('{"reason":"OTHER","tsp_id":12}')
    worker._dispatch(message)
    assert worker.liveHandler.values == [12]


def test_dispatch_calls_the_handler_for_each_reason(serviceConfig):
    worker = listener(serviceConfig)
    worker._dispatch({"reason": "TRAIN", "tspId": 1})
    worker._dispatch({"reason": "TRACER", "tspId": None})
    worker._dispatch({"reason": "LIVE", "tspId": 2})
    assert worker.trainHandler.values == [1]
    assert worker.tracerHandler.values == [None]
    assert worker.liveHandler.values == [2]


def test_successful_message_is_deleted(serviceConfig):
    sqs = SqsFake()
    worker = listener(serviceConfig, sqs)
    worker._processMessage(
        {
            "Body": json.dumps({"reason": "TRAIN", "tsp_id": 3}),
            "ReceiptHandle": "receipt",
        }
    )
    assert sqs.deleted[0]["ReceiptHandle"] == "receipt"


def test_invalid_or_failed_message_is_not_deleted(serviceConfig):
    sqs = SqsFake()
    worker = listener(serviceConfig, sqs)
    worker._processMessage({"Body": "bad", "ReceiptHandle": "one"})

    class FailingHandler:
        def process(self, value):
            raise RuntimeError("temporary failure")

    worker.trainHandler = FailingHandler()
    worker._processMessage(
        {
            "Body": json.dumps({"reason": "TRAIN", "tsp_id": 3}),
            "ReceiptHandle": "two",
        }
    )
    assert sqs.deleted == []


def test_listener_keeps_receiving_messages(serviceConfig):
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
    worker = listener(serviceConfig, queue)
    with pytest.raises(KeyboardInterrupt):
        worker.run()
    assert queue.calls == 2
