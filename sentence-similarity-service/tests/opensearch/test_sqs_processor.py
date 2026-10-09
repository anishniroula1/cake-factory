from similarity_completed.opensearch.errors import OperationFailedError
from similarity_completed.opensearch.sqs_processor import SqsProcessor
from tests.opensearch.fakes import LoggerFake, SqsFake


class Handler:
    def __init__(self, fails=False):
        self.fails = fails
        self.processed = []

    def process(self, tsp_id):
        if self.fails:
            raise OperationFailedError("temporary failure")
        self.processed.append(tsp_id)
        return {"tsp_id": tsp_id}


class Dependencies:
    def __init__(self, handler):
        self.aws_sqs = SqsFake()
        self.sentence_similarity_handler = handler
        self.logger = LoggerFake()


def test_invalid_or_failed_messages_are_not_deleted():
    invalid = Dependencies(Handler())
    failed = Dependencies(Handler(fails=True))

    SqsProcessor(invalid)._process_message({"MessageId": "missing"})
    SqsProcessor(failed)._process_message({"tsp_id": 10, "MessageId": "failed", "ReceiptHandle": "receipt"})

    assert invalid.aws_sqs.deleted == []
    assert failed.aws_sqs.deleted == []


def test_delete_failure_is_logged_after_success():
    dependencies = Dependencies(Handler())
    dependencies.aws_sqs.delete_messages = lambda message: False
    processor = SqsProcessor(dependencies)

    processor._process_message({"tsp_id": 10, "MessageId": "message", "ReceiptHandle": "receipt"})

    assert dependencies.sentence_similarity_handler.processed == [10]
    assert dependencies.logger.generic_messages == ["tsp_id=10"]
    assert any(level == "error" for level, message in dependencies.logger.messages)
