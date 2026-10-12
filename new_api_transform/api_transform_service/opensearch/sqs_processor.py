import json

from api_transform_service.opensearch.contracts import SqsMessage, TspId
from api_transform_service.opensearch.dependencies import Dependencies
from api_transform_service.opensearch.errors import ApiTransformError
from ta_utils.aws.sqs.AwsSQS import AwsSQS


class SqsProcessor:
    def __init__(self, dependencies: Dependencies) -> None:
        """Keep the dependencies used to process one SQS message."""

        self.dependencies = dependencies

    def run(self) -> None:
        """Read queue messages and process each document separately."""

        while True:
            messages, source_queue = self._receive_messages()
            for message in messages:
                self._process_message(message, source_queue)

    def _receive_messages(self) -> tuple[list[SqsMessage], AwsSQS]:
        """Read the main queue first, then read a DLQ list when it is empty.

        The main queue is configured to return one document. A DLQ receive can
        return several messages, and every message is still processed alone.
        """

        messages = self.dependencies.aws_sqs.listen_sqs_message()
        if messages or self.dependencies.dlq_sqs is None:
            return messages, self.dependencies.aws_sqs
        return self.dependencies.dlq_sqs.listen_sqs_message(), self.dependencies.dlq_sqs

    def _process_message(self, message: SqsMessage, source_queue: AwsSQS | None = None) -> bool:
        """Process one document and delete its message only after success.

        Input: one SQS message containing tsp_id=10.
        Output: True after the complete flow succeeds; otherwise False.
        """

        tsp_id = self._read_tsp_id(message)
        if tsp_id is None:
            self._leave_message_for_sqs("The SQS message is missing the required tsp_id.")
            return False

        self.dependencies.logger.set_generic_message(f"tsp_id={tsp_id}")
        self.dependencies.logger.info("Starting an API Transform document message.")
        try:
            result = self.dependencies.api_transform_handler.process(tsp_id)
            if not result:
                self._leave_message_for_sqs("API Transform did not complete the document.")
                return False

            if not self._send_next_message(tsp_id):
                self._leave_message_for_sqs("The completed document could not be sent to the next queue.")
                return False

            queue = source_queue or self.dependencies.aws_sqs
            if not queue.delete_messages(message):
                self.dependencies.logger.error("Processing completed, but source message deletion failed.")
                return False

            self.dependencies.logger.info("API Transform completed and deleted the source SQS message.")
            return True
        except ApiTransformError as error:
            self.dependencies.logger.error(
                f"API Transform stopped because of {type(error).__name__}: {str(error)[:500]}"
            )
            return False
        finally:
            self.dependencies.logger.set_generic_message(None)

    def _send_next_message(self, tsp_id: TspId) -> bool:
        """Send the completed document to the next service."""

        body = {"source": "P2", "tsp_id": str(tsp_id), "sent_by": "api-transform"}
        return self.dependencies.sent_sqs.send_sqs_message(
            send_queue_url=self.dependencies.config.sent_sqs_endpoint, message_body=json.dumps(body)
        )

    def _leave_message_for_sqs(self, reason: str) -> None:
        """Log that SQS will retry this message and eventually route it to the DLQ."""

        self.dependencies.logger.error(
            f"{reason} The source message was not deleted; SQS will use its configured retry and DLQ flow."
        )

    @staticmethod
    def _read_tsp_id(message: SqsMessage) -> TspId | None:
        """Return tsp_id exactly as supplied, or None when it is missing."""

        tsp_id = message.get("tsp_id")
        if tsp_id is None or tsp_id == "":
            return None
        return tsp_id
