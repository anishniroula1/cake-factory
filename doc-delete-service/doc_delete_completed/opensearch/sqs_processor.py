from doc_delete_completed.opensearch.contracts import SqsMessage, TspId
from doc_delete_completed.opensearch.dependencies import Dependencies
from doc_delete_completed.opensearch.errors import OperationFailedError, PermanentDataError, QueueSendError
from doc_delete_completed.opensearch.retry import safe_error_summary


class SqsProcessor:
    """Receive one document message at a time and coordinate its deletion."""

    def __init__(self, dependencies: Dependencies) -> None:
        self.dependencies = dependencies

    def run(self) -> None:
        """Continuously receive and process document-deletion messages."""

        while True:
            for message in self.dependencies.aws_sqs.listen_sqs_message():
                self._process_document_delete_message(message)

    def _process_document_delete_message(self, message: SqsMessage) -> None:
        """Delete one document and remove its source message only after success.

        Input: message={"tsp_id": 8205, "ReceiptHandle": "receipt-value"}

        A failed message is not deleted. SQS can deliver it again and apply the
        queue's configured DLQ policy.
        """

        try:
            tsp_id = self._read_tsp_id_from_message(message)
            if tsp_id is None:
                self.dependencies.logger.error("The SQS message is missing the required tsp_id.")
                return
            self.dependencies.logger.set_generic_message(f"tsp_id={tsp_id}")
            self.dependencies.logger.info("Starting an SQS document-deletion message.")
            deletion_result = self.dependencies.document_delete_handler.process_document_deletion(tsp_id)
            if not deletion_result.get("next_message_sent"):
                self.dependencies.logger.error("Document deletion did not complete the message.")
                return

            if not self.dependencies.aws_sqs.delete_messages(message):
                self.dependencies.logger.error("Processing completed, but source message deletion failed.")
                return
            self.dependencies.logger.info("Document deletion completed and deleted the source SQS message.")
        except (OperationFailedError, PermanentDataError, QueueSendError) as error:
            self.dependencies.logger.error(
                f"Document deletion stopped because of {safe_error_summary(error)}"
            )
            return
        finally:
            self.dependencies.logger.set_generic_message(None)

    @staticmethod
    def _read_tsp_id_from_message(message: SqsMessage) -> TspId | None:
        """Return tsp_id without changing the integer or string supplied upstream."""

        tsp_id = message.get("tsp_id")
        return None if tsp_id is None or tsp_id == "" else tsp_id
