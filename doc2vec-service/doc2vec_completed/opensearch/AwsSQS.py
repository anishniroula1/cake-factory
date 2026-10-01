import json
import time
import uuid

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from doc2vec_completed.opensearch.contracts import (
    NextQueueMessage,
    RawSqsMessage,
    SqsMessage,
)


class AwsSQS:
    def __init__(
        self,
        queue_url: str | None,
        aws_region: str = "us-east-1",
        aws_endpoint_url: str | None = None,
        max_number_of_messages: int = 1,
        wait_time: int = 20,
        logger=None,
        visibility_timeout: int = 7_200,
        retry_wait_seconds: int = 5,
        sqs_client=None,
    ) -> None:
        """Create the small SQS wrapper used by the historical worker."""

        self._logger = logger
        self._sqs = sqs_client or boto3.client(
            "sqs",
            region_name=aws_region,
            endpoint_url=aws_endpoint_url,
        )
        self._queue_url = queue_url
        self._max_number_of_messages = max_number_of_messages
        self._wait_time = wait_time
        self._visibility_timeout = visibility_timeout
        self._retry_wait_seconds = retry_wait_seconds

    def receive_messages(self) -> list[RawSqsMessage]:
        """Receive messages and return an empty list after a temporary failure."""

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
            self._logger.error(
                f"Failed to receive from SQS queue {self._queue_url}: "
                f"{str(error)[:500]}"
            )
            time.sleep(self._retry_wait_seconds)
            return []

        messages = response.get("Messages", [])
        if messages:
            self._logger.info(
                f"Received {len(messages)} message(s) from SQS queue {self._queue_url}"
            )
        return messages

    def sqs_message_to_json(self, message: RawSqsMessage) -> SqsMessage:
        """Convert one raw SQS body and keep the fields needed for deletion."""

        if not message:
            return {}

        try:
            message_object = json.loads(message.get("Body", ""))
            # Some publishers encode the JSON body twice. Decode that second
            # layer without stripping valid slashes or quotation marks.
            if isinstance(message_object, str):
                message_object = json.loads(message_object)
            if not isinstance(message_object, dict):
                raise TypeError("SQS body must be a JSON object")

            receive_count = message.get("Attributes", {}).get(
                "ApproximateReceiveCount",
                "0",
            )
            message_id = message.get("MessageId")
            self._logger.info(
                f"SQS message_id={message_id} receive_count={receive_count}"
            )
            return {
                **message_object,
                "MessageId": message_id,
                "ReceiptHandle": message.get("ReceiptHandle"),
                "message_receive_count": int(receive_count),
            }
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            self._logger.error(
                f"SQS message_id={message.get('MessageId')} has an invalid "
                f"JSON body: {str(error)[:500]}"
            )
            return {}

    def listen_sqs_message(self) -> list[SqsMessage]:
        """Receive raw SQS messages and return only valid JSON messages."""

        formatted_messages: list[SqsMessage] = []
        for message in self.receive_messages():
            formatted = self.sqs_message_to_json(message)
            if formatted:
                formatted_messages.append(formatted)
        return formatted_messages

    def delete_messages(self, message: SqsMessage) -> bool:
        """Delete one successfully processed source message."""

        try:
            self._sqs.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )
            self._logger.info(
                f"Deleted SQS message_id={message.get('MessageId')} from "
                f"queue {self._queue_url}"
            )
            return True
        except (BotoCoreError, ClientError, KeyError) as error:
            self._logger.error(
                f"Failed to delete SQS message_id={message.get('MessageId')} "
                f"from queue {self._queue_url}: {str(error)[:500]}"
            )
            return False

    def send_sqs_message(
        self,
        send_queue_url: str,
        message_body: str | NextQueueMessage,
        wait_time: int = 5,
        retries: int = 0,
        is_fifo: bool = True,
    ) -> bool:
        """Send one message and retry the failed send when requested."""

        body = (
            message_body if isinstance(message_body, str) else json.dumps(message_body)
        )
        for attempt in range(retries + 1):
            try:
                parameters = {
                    "QueueUrl": send_queue_url,
                    "MessageBody": body,
                }
                if is_fifo:
                    parameters["MessageGroupId"] = uuid.uuid4().hex
                    parameters["MessageDeduplicationId"] = uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"{send_queue_url}:{body}",
                    ).hex

                response = self._sqs.send_message(**parameters)
                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if status == 200:
                    self._logger.info(f"Sent SQS message to queue {send_queue_url}")
                    return True
                error_message = f"SQS returned HTTP status {status}"
            except (BotoCoreError, ClientError) as error:
                error_message = str(error)[:500]

            self._logger.error(
                f"Failed to send SQS message to queue {send_queue_url}: {error_message}"
            )
            if attempt < retries:
                self._logger.info(f"Retrying SQS send to queue {send_queue_url}")
                time.sleep(wait_time)

        return False
