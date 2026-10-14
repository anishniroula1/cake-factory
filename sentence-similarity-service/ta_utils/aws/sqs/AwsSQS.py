import json
import time
import uuid

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from similarity_completed.opensearch.contracts import NextQueueMessage, RawSqsMessage, SqsMessage


class AwsSQS:
    """Provide the SQS receive, send, and delete operations used by the service."""

    def __init__(
        self,
        queue_url: str | None,
        aws_region: str = "us-east-1",
        aws_endpoint_url: str | None = None,
        max_number_of_messages: int = 1,
        wait_time: int = 20,
        logger=None,
        visibility_timeout: int = 7200,
    ) -> None:
        self._logger = logger
        self._sqs = boto3.client("sqs", region_name=aws_region, endpoint_url=aws_endpoint_url)
        self._queue_url = queue_url
        self._max_number_of_messages = max_number_of_messages
        self._wait_time = wait_time
        self._visibility_timeout = visibility_timeout

    def receive_messages(self) -> list[RawSqsMessage]:
        """Receive raw messages from the configured queue. boto3 delivery
        metadata remains attached so completed messages can be deleted by their
        receipt handles.

        Output: raw boto3 messages containing MessageId, Body, ReceiptHandle,
        and delivery attributes.

        An empty list is returned when no message is available or the receive
        request fails.
        """

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
            self._logger.error(f"Failed to receive an SQS message: {str(error)[:500]}")
            return []
        return response.get("Messages", [])

    def sqs_message_to_json(self, message: RawSqsMessage) -> SqsMessage:
        """Decode an SQS body and attach delivery metadata.

        Input: message.Body="{\"tsp_id\": 8205}"
        Output: {"tsp_id": 8205, "MessageId": "abc",
        "ReceiptHandle": "receipt", "message_receive_count": 1}

        An invalid body returns an empty mapping and writes an error log.
        """

        if not message:
            return {}
        try:
            body = json.loads(message.get("Body", ""))
            if isinstance(body, str):
                body = json.loads(body)
            if not isinstance(body, dict):
                raise TypeError("SQS body must be a JSON object")
            receive_count = message.get("Attributes", {}).get("ApproximateReceiveCount", "0")
            return {
                **body,
                "MessageId": message.get("MessageId"),
                "ReceiptHandle": message.get("ReceiptHandle"),
                "message_receive_count": int(receive_count),
            }
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            self._logger.error(f"Received an invalid SQS JSON body: {str(error)[:500]}")
            return {}

    def listen_sqs_message(self) -> list[SqsMessage]:
        """Receive raw SQS records and retain valid JSON object messages.

        Output: decoded messages ready for processing, such as
        [{"tsp_id": 8205, "ReceiptHandle": "receipt"}].
        """

        messages = []
        for raw_message in self.receive_messages():
            message = self.sqs_message_to_json(raw_message)
            if message:
                messages.append(message)
        return messages

    def delete_messages(self, message: SqsMessage) -> bool:
        """Delete a processed source message from the configured queue.

        Input: message={"ReceiptHandle": "receipt-value"}
        Output: True when SQS accepts the delete request; otherwise False.
        """

        try:
            self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=message["ReceiptHandle"])
            return True
        except (BotoCoreError, ClientError, KeyError) as error:
            self._logger.error(f"Failed to delete the completed SQS message: {str(error)[:500]}")
            return False

    def send_sqs_message(
        self,
        send_queue_url: str,
        message_body: str | NextQueueMessage,
        wait_time: int = 5,
        retries: int = 0,
        is_fifo: bool = True,
    ) -> bool:
        """Send a message to a downstream queue with optional retries. FIFO
        destinations receive generated group and deduplication identifiers.

        Input: send_queue_url="https://sqs.../next.fifo",
        message_body={"tsp_id": "8205"}, wait_time=5, retries=2, is_fifo=True
        Output: True when SQS returns HTTP 200; otherwise False after all attempts.

        retries=2 permits three total send attempts.
        """

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
                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if status == 200:
                    return True
                error_message = f"SQS returned HTTP status {status}"
            except (BotoCoreError, ClientError) as error:
                error_message = str(error)[:500]

            self._logger.error(f"Failed to send the next SQS message: {error_message}")
            if attempt < retries:
                time.sleep(wait_time)
        return False
