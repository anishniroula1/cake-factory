import pytest

from similarity_completed.opensearch import main
from tests.opensearch.fakes import LoggerFake, SqsFake


class StopApplication(Exception):
    pass


class OneMessageSqs(SqsFake):
    def listen_sqs_message(self):
        self.polls += 1
        if self.polls == 1:
            return self.messages
        raise StopApplication


class Handler:
    def __init__(self):
        self.processed = []

    def process(self, tsp_id):
        self.processed.append(tsp_id)


class Dependencies:
    def __init__(self, queue, handler):
        self.aws_sqs = queue
        self.sentence_similarity_handler = handler
        self.logger = LoggerFake()
        self.closed = False

    def close(self):
        self.closed = True


def test_main_processes_one_message_deletes_it_and_closes():
    message = {
        "tsp_id": "TSP-10",
        "MessageId": "message-1",
        "ReceiptHandle": "receipt-1",
    }
    queue = OneMessageSqs([message])
    handler = Handler()
    dependencies = Dependencies(queue, handler)

    with pytest.raises(StopApplication):
        main.run_opensearch_flow(dependencies=dependencies)

    assert handler.processed == ["TSP-10"]
    assert queue.deleted == [message]
    assert dependencies.closed is True
