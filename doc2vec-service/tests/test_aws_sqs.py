import json

from botocore.exceptions import ClientError

from doc2vec_service.AwsSQS import AwsSQS


class SqsClient:
    def __init__(self):
        self.received = []
        self.sent = []
        self.deleted = []
        self.messages = []
        self.sendStatuses = [200]

    def receive_message(self, **values):
        self.received.append(values)
        return {"Messages": self.messages}

    def send_message(self, **values):
        self.sent.append(values)
        status = self.sendStatuses.pop(0)
        return {"ResponseMetadata": {"HTTPStatusCode": status}}

    def delete_message(self, **values):
        self.deleted.append(values)


def awsSqs(client, customLogger):
    return AwsSQS(
        queue_url="source-queue",
        logger=customLogger,
        retry_wait_seconds=1,
        sqs_client=client,
    )


def test_receive_and_format_sqs_message(customLogger):
    client = SqsClient()
    client.messages = [
        {
            "Body": json.dumps({"reason": "HISTORICAL", "tsp_id": 10}),
            "MessageId": "message-1",
            "ReceiptHandle": "receipt-1",
            "Attributes": {"ApproximateReceiveCount": "2"},
        }
    ]
    wrapper = awsSqs(client, customLogger)

    messages = wrapper.listen_sqs_message()

    assert messages[0]["tsp_id"] == 10
    assert messages[0]["message_receive_count"] == 2
    assert messages[0]["ReceiptHandle"] == "receipt-1"
    assert client.received[0]["MaxNumberOfMessages"] == 1
    assert client.received[0]["MessageAttributeNames"] == ["All"]


def test_message_formatter_accepts_double_encoded_json_and_rejects_bad_json(
    customLogger,
):
    wrapper = awsSqs(SqsClient(), customLogger)
    doubleEncoded = json.dumps(json.dumps({"tsp_id": 12}))

    assert wrapper.sqs_message_to_json({"Body": doubleEncoded})["tsp_id"] == 12
    assert wrapper.sqs_message_to_json({"Body": "bad"}) == {}
    assert wrapper.sqs_message_to_json({"Body": "[]"}) == {}
    assert wrapper.sqs_message_to_json({}) == {}


def test_receive_failure_returns_an_empty_list(
    customLogger,
    monkeypatch,
):
    class FailedClient(SqsClient):
        def receive_message(self, **values):
            raise ClientError(
                {"Error": {"Code": "500", "Message": "receive failed"}},
                "ReceiveMessage",
            )

    monkeypatch.setattr("doc2vec_service.AwsSQS.time.sleep", lambda value: None)
    wrapper = awsSqs(FailedClient(), customLogger)

    assert wrapper.receive_messages() == []
    assert len(customLogger.errorMessages) == 1


def test_delete_message_returns_success_or_failure(customLogger):
    client = SqsClient()
    wrapper = awsSqs(client, customLogger)
    message = {"MessageId": "one", "ReceiptHandle": "receipt"}

    assert wrapper.delete_messages(message)
    assert client.deleted[0]["ReceiptHandle"] == "receipt"

    assert not wrapper.delete_messages({"MessageId": "missing-receipt"})


def test_send_fifo_and_standard_messages(customLogger):
    client = SqsClient()
    client.sendStatuses = [200, 200]
    wrapper = awsSqs(client, customLogger)

    assert wrapper.send_sqs_message("next.fifo", {"tsp_id": 10})
    assert "MessageGroupId" in client.sent[0]
    assert "MessageDeduplicationId" in client.sent[0]

    assert wrapper.send_sqs_message(
        "next-standard",
        "already-json",
        is_fifo=False,
    )
    assert "MessageGroupId" not in client.sent[1]
    assert client.sent[1]["MessageBody"] == "already-json"


def test_send_retries_a_failed_status(customLogger, monkeypatch):
    client = SqsClient()
    client.sendStatuses = [500, 200]
    wrapper = awsSqs(client, customLogger)
    monkeypatch.setattr("doc2vec_service.AwsSQS.time.sleep", lambda value: None)

    assert wrapper.send_sqs_message("next.fifo", {}, retries=1)
    assert len(client.sent) == 2

    client.sendStatuses = [500]
    assert not wrapper.send_sqs_message("next.fifo", {})


def test_send_returns_false_after_an_aws_exception(customLogger):
    class FailedClient(SqsClient):
        def send_message(self, **values):
            raise ClientError(
                {"Error": {"Code": "500", "Message": "send failed"}},
                "SendMessage",
            )

    wrapper = awsSqs(FailedClient(), customLogger)
    assert not wrapper.send_sqs_message("next.fifo", {})
