from doc_delete_completed.opensearch import constants
from doc_delete_completed.opensearch.db_store.text_similarity_input_db_store import TextSimilarityInputDBStore
from doc_delete_completed.opensearch.errors import OperationFailedError, PermanentDataError, QueueSendError
from doc_delete_completed.opensearch.handlers.document_delete_handler import DocumentDeleteHandler
from doc_delete_completed.opensearch.retry import retry_operation, safe_error_summary
from ta_utils.logger.CustomLogger import CustomLogger


class DocumentDeleteJob:
    """Process every document currently marked READY_FOR_DELETION."""

    def __init__(
        self,
        text_similarity_input_db_store: TextSimilarityInputDBStore,
        document_delete_handler: DocumentDeleteHandler,
        custom_logger: CustomLogger,
    ) -> None:
        self.text_similarity_input_db_store = text_similarity_input_db_store
        self.document_delete_handler = document_delete_handler
        self.logger = custom_logger

    def run(self) -> int:
        """Delete ordered database records until no pending documents remain.

        Output: the number of documents completed by this scheduled run.

        A failed document stops the run. Its state remains READY_FOR_DELETION,
        so the next scheduled run starts with that document again.
        """

        processed_document_count = 0
        after_tsp_id: int | None = None
        self.logger.info("Document Delete job started.")

        while True:
            tsp_ids = retry_operation(
                "Load documents ready for deletion",
                self.text_similarity_input_db_store.load_ready_for_deletion_tsp_ids,
                after_tsp_id,
                constants.DOCUMENT_DELETE_BATCH_SIZE,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
                max_attempts=constants.MAX_ATTEMPTS,
            )
            if not tsp_ids:
                self.logger.info(
                    f"Document Delete job completed {processed_document_count} document(s)."
                )
                return processed_document_count

            self.logger.info(f"Loaded {len(tsp_ids)} document(s) ready for deletion.")
            for tsp_id in tsp_ids:
                self._process_document(tsp_id)
                processed_document_count += 1
            after_tsp_id = tsp_ids[-1]

    def _process_document(self, tsp_id: int) -> None:
        """Delete one document and send its completion event to the next queue."""

        self.logger.set_generic_message(f"tsp_id={tsp_id}")
        try:
            self.logger.info("Starting document deletion.")
            deletion_result = self.document_delete_handler.process_document_deletion(tsp_id)
            if not deletion_result.get("next_message_sent"):
                raise OperationFailedError("document deletion did not send the next SQS message")
            self.logger.info("Document deletion completed and sent the next SQS message.")
        except (OperationFailedError, PermanentDataError, QueueSendError) as error:
            self.logger.error(f"Document deletion stopped because of {safe_error_summary(error)}")
            raise
        finally:
            self.logger.set_generic_message(None)
