import json

from ta_utils.aws.sqs.AwsSQS import AwsSQS
from transform_completed.opensearch.contracts import SqsMessage, TspId
from transform_completed.opensearch.dependencies import Dependencies
from transform_completed.opensearch.errors import ApiTransformError
from transform_completed.opensearch.retry import safe_error_summary


class SqsProcessor:
    """Read document messages and run the complete API Transform flow for each one."""

    def __init__(self, dependencies: Dependencies) -> None:
        self.dependencies = dependencies

    def run(self) -> None:
        """Keep reading queue messages and process each document separately."""

        while True:
            messages, source_queue = self._receive_source_or_dlq_messages()
            for message in messages:
                self._process_document_message(message, source_queue)

    def _receive_source_or_dlq_messages(self) -> tuple[list[SqsMessage], AwsSQS]:
        """Read the main queue first, then read the DLQ when the main queue is empty.

        Output: the received message list and the queue client that owns those
        messages. The main queue returns one document, while the DLQ can return
        several documents that are still processed one at a time.
        """

        messages = self.dependencies.aws_sqs.listen_sqs_message()
        if messages or self.dependencies.dlq_sqs is None:
            return messages, self.dependencies.aws_sqs
        return self.dependencies.dlq_sqs.listen_sqs_message(), self.dependencies.dlq_sqs

    def _process_document_message(self, message: SqsMessage, source_queue: AwsSQS | None = None) -> bool:
        """Process one document and delete its message only after success.

        Input: one SQS message containing tsp_id=10.
        Output: True after the complete flow succeeds; otherwise False.
        """

        tsp_id = self._read_tsp_id_from_message(message)
        if tsp_id is None:
            self._log_undelivered_message("The SQS message is missing the required tsp_id.")
            return False

        self.dependencies.logger.set_generic_message(f"tsp_id={tsp_id}")
        self.dependencies.logger.info("Starting an API Transform document message.")
        try:
            transform_result = self.dependencies.api_transform_handler.process_document(tsp_id)
            if not transform_result:
                self._log_undelivered_message("API Transform did not complete the document.")
                return False

            if not self._send_document_to_next_queue(tsp_id):
                self._log_undelivered_message("The completed document could not be sent to the next queue.")
                return False

            message_source_queue = source_queue or self.dependencies.aws_sqs
            if not message_source_queue.delete_messages(message):
                self.dependencies.logger.error("Processing completed, but source message deletion failed.")
                return False

            self.dependencies.logger.info("API Transform completed and deleted the source SQS message.")
            return True
        except ApiTransformError as error:
            self.dependencies.logger.error(
                f"API Transform stopped because of {safe_error_summary(error)}"
            )
            return False
        finally:
            self.dependencies.logger.set_generic_message(None)

    def _send_document_to_next_queue(self, tsp_id: TspId) -> bool:
        """Send the completed document unless it came from a backfill queue.

        Input: tsp_id=10
        Output: True when SQS accepts the message or the receiving queue URL
        contains backfill; otherwise False.
        """

        if "backfill" in self.dependencies.config.receive_sqs_endpoint.lower():
            self.dependencies.logger.info(
                "The next SQS event was skipped because this document came from a backfill queue."
            )
            return True

        next_message_body = {"source": "P2", "tsp_id": str(tsp_id), "sent_by": "api-transform"}
        self.dependencies.logger.info("Sending the API Transform completion event to the next SQS queue.")
        message_sent = self.dependencies.sent_sqs.send_sqs_message(
            send_queue_url=self.dependencies.config.sent_sqs_endpoint,
            message_body=json.dumps(next_message_body),
        )
        if message_sent:
            self.dependencies.logger.info("The next SQS queue accepted the API Transform completion event.")
        return message_sent

    def _log_undelivered_message(self, reason: str) -> None:
        """Log why the current source message cannot be deleted."""

        self.dependencies.logger.error(
            f"{reason} The source message was not deleted; SQS will use its configured retry and DLQ flow."
        )

    @staticmethod
    def _read_tsp_id_from_message(message: SqsMessage) -> TspId | None:
        """Return tsp_id exactly as supplied, or None when it is missing.

        Input: message={"tsp_id": "10"}
        Output: "10"
        """

        tsp_id = message.get("tsp_id")
        if tsp_id is None or tsp_id == "":
            return None
        return tsp_id
