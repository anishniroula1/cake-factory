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
    """Provide the SQS receive, send, and delete operations used by Doc2Vec."""

    def __init__(
        self,
        queue_url: str | None,
        aws_region: str = "us-east-1",
        aws_endpoint_url: str | None = None,
        max_number_of_messages: int = 1,
        wait_time: int = 20,
        logger=None,
        visibility_timeout: int = 7_200,
        sqs_client=None,
    ) -> None:
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

    def receive_messages(self) -> list[RawSqsMessage]:
        """Receive raw messages from the configured source queue.

        Output: boto3 messages containing Body, MessageId, and ReceiptHandle.

        An empty list is returned when no message is available or the request
        fails.
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
            self._logger.error(
                f"Failed to receive from SQS queue {self._queue_url}: "
                f"{str(error)[:500]}"
            )
            return []

        messages = response.get("Messages", [])
        if messages:
            self._logger.info(
                f"Received {len(messages)} message(s) from SQS queue {self._queue_url}"
            )
        return messages

    def sqs_message_to_json(self, message: RawSqsMessage) -> SqsMessage:
        """Decode one SQS JSON body and attach its delivery metadata.

        Input: message.Body="{\"reason\": \"HISTORICAL\", \"tsp_id\": 1040}"
        Output: {"reason": "HISTORICAL", "tsp_id": 1040,
        "MessageId": "abc", "ReceiptHandle": "receipt", "message_receive_count": 1}

        Invalid JSON returns an empty mapping.
        """

        if not message:
            return {}

        try:
            message_object = json.loads(message.get("Body", ""))
            # A message may contain JSON text inside another JSON value.
            # Decode the second value so the result is one normal dictionary.
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
        """Receive SQS records and retain messages with valid JSON object bodies.

        Output: decoded messages ready for processing, or an empty list.
        """

        formatted_messages: list[SqsMessage] = []
        for message in self.receive_messages():
            formatted = self.sqs_message_to_json(message)
            if formatted:
                formatted_messages.append(formatted)
        return formatted_messages

    def delete_messages(self, message: SqsMessage) -> bool:
        """Delete one source message after its document finishes successfully.

        Input: message={"MessageId": "abc", "ReceiptHandle": "receipt"}
        Output: True when SQS accepts the delete request; otherwise False.
        """

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
        """Send one message to a downstream queue with optional retries.

        Input: send_queue_url="https://sqs.../similarity.fifo",
        message_body={"reason": "HISTORICAL", "tsp_id": 1040}, retries=2
        Output: True when SQS returns HTTP 200; otherwise False after all attempts.
        """

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

    def send_batch_messages(
        self,
        send_queue_url: str,
        batch_messages: list[str | NextQueueMessage],
        wait_time: int = 5,
        retries: int = 0,
        is_fifo: bool = True,
        **kwargs,
    ) -> dict[str, object]:
        """Send messages with the SQS batch API in groups of at most ten.

        Input: send_queue_url="https://sqs.../similarity.fifo",
        batch_messages=[{"reason": "HISTORICAL", "tsp_id": 1040}]
        Output: {"status": "SUCCEED"} when every batch entry is accepted.

        Amazon SQS accepts no more than ten entries in one batch request. A
        larger input list is therefore split into multiple API calls.
        """

        if not batch_messages:
            return {"status": "SUCCEED"}
        if not send_queue_url:
            return {"status": "FAILED", "data": batch_messages}

        for offset in range(0, len(batch_messages), 10):
            message_batch = batch_messages[offset : offset + 10]
            entries = []
            for position, message in enumerate(message_batch):
                body = message if isinstance(message, str) else json.dumps(message)
                entry = {
                    "Id": f"message-{offset + position}",
                    "MessageBody": body,
                }
                if is_fifo:
                    entry["MessageGroupId"] = kwargs.get(
                        "message_group_id", "doc2vec-historical"
                    )
                    entry["MessageDeduplicationId"] = uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"{send_queue_url}:{body}",
                    ).hex
                entries.append(entry)

            for attempt in range(retries + 1):
                try:
                    response = self._sqs.send_message_batch(
                        QueueUrl=send_queue_url,
                        Entries=entries,
                    )
                    status = response.get("ResponseMetadata", {}).get(
                        "HTTPStatusCode"
                    )
                    failed_entries = response.get("Failed", [])
                    if status == 200 and not failed_entries:
                        break
                    error_message = (
                        f"SQS returned HTTP status {status} with "
                        f"{len(failed_entries)} failed batch entry(s)"
                    )
                except (BotoCoreError, ClientError) as error:
                    error_message = str(error)[:500]

                self._logger.error(
                    "Failed to send an SQS message batch to queue "
                    f"{send_queue_url}: {error_message}"
                )
                if attempt < retries:
                    time.sleep(wait_time)
            else:
                return {"status": "FAILED", "data": message_batch}

        self._logger.info(
            f"Sent {len(batch_messages)} SQS message(s) to queue {send_queue_url}"
        )
        return {"status": "SUCCEED"}

