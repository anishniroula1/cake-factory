import json
import time
import uuid
from typing import TypedDict

import boto3
from botocore.exceptions import BotoCoreError, ClientError


class SqsMessage(TypedDict, total=False):
    tsp_id: int | str
    source: str
    sent_by: str
    reason: str
    MessageId: str
    ReceiptHandle: str
    message_receive_count: int


class NextQueueMessage(TypedDict):
    source: str
    tsp_id: str
    sent_by: str


class RawSqsMessage(TypedDict, total=False):
    Body: str
    MessageId: str
    ReceiptHandle: str
    Attributes: dict[str, str]


class AwsSQS:
    """Provide the receive, send, and delete operations used by document deletion."""

    def __init__(
        self,
        queue_url: str | None,
        aws_region: str = "us-east-1",
        aws_endpoint_url: str | None = None,
        max_number_of_messages: int = 1,
        wait_time: int = 20,
        logger=None,
        visibility_timeout: int = 7200,
        retry_wait_seconds: int = 5,
        sqs_client=None,
    ) -> None:
        self._logger = logger
        self._sqs = sqs_client or boto3.client("sqs", region_name=aws_region, endpoint_url=aws_endpoint_url)
        self._queue_url = queue_url
        self._max_number_of_messages = max_number_of_messages
        self._wait_time = wait_time
        self._visibility_timeout = visibility_timeout
        self._retry_wait_seconds = retry_wait_seconds

    def receive_messages(self) -> list[RawSqsMessage]:
        """Receive one raw message; a temporary AWS failure returns an empty list."""

        try:
            response = self._sqs.receive_message(
                QueueUrl=self._queue_url,
                MaxNumberOfMessages=self._max_number_of_messages,
                WaitTimeSeconds=self._wait_time,
                MessageAttributeNames=["All"],
                AttributeNames=["ApproximateReceiveCount"],
                VisibilityTimeout=self._visibility_timeout,
            )
        except (BotoCoreError, ClientError) as error:
            self._logger.error(f"Failed to receive from SQS: {str(error)[:500]}")
            time.sleep(self._retry_wait_seconds)
            return []
        return response.get("Messages", [])

    def sqs_message_to_json(self, message: RawSqsMessage) -> SqsMessage:
        """Decode one SQS JSON body and attach its receipt metadata."""

        try:
            value = json.loads(message.get("Body", ""))
            if isinstance(value, str):
                value = json.loads(value)
            if not isinstance(value, dict):
                raise TypeError("SQS body must be a JSON object")
            return {
                **value,
                "MessageId": message.get("MessageId"),
                "ReceiptHandle": message.get("ReceiptHandle"),
                "message_receive_count": int(message.get("Attributes", {}).get("ApproximateReceiveCount", "0")),
            }
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            self._logger.error(f"SQS message has invalid JSON: {str(error)[:500]}")
            return {}

    def listen_sqs_message(self) -> list[SqsMessage]:
        """Return decoded messages with valid JSON object bodies."""

        return [formatted for raw in self.receive_messages() if (formatted := self.sqs_message_to_json(raw))]

    def delete_messages(self, message: SqsMessage) -> bool:
        """Delete one source message after all deletion work succeeds."""

        try:
            self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=message["ReceiptHandle"])
            return True
        except (BotoCoreError, ClientError, KeyError) as error:
            self._logger.error(f"Failed to delete the source SQS message: {str(error)[:500]}")
            return False

    def send_sqs_message(
        self,
        send_queue_url: str,
        message_body: str | NextQueueMessage,
        wait_time: int = 5,
        retries: int = 0,
        is_fifo: bool = True,
    ) -> bool:
        """Send one message to the next service with optional retries."""

        body = message_body if isinstance(message_body, str) else json.dumps(message_body)
        for attempt in range(retries + 1):
            try:
                parameters = {"QueueUrl": send_queue_url, "MessageBody": body}
                if is_fifo:
                    parameters["MessageGroupId"] = uuid.uuid4().hex
                    parameters["MessageDeduplicationId"] = uuid.uuid5(
                        uuid.NAMESPACE_URL, f"{send_queue_url}:{body}"
                    ).hex
                response = self._sqs.send_message(**parameters)
                if response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
                    return True
                error_message = f"SQS returned HTTP {response.get('ResponseMetadata', {}).get('HTTPStatusCode')}"
            except (BotoCoreError, ClientError) as error:
                error_message = str(error)[:500]
            self._logger.error(f"Failed to send the next SQS message: {error_message}")
            if attempt < retries:
                time.sleep(wait_time)
        return False
