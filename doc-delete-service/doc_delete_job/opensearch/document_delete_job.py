import json

from doc_delete_job.opensearch.contracts import TspId
from doc_delete_job.opensearch.errors import OperationFailedError, PermanentDataError, QueueSendError
from doc_delete_job.opensearch.handlers.document_delete_handler import DocumentDeleteHandler
from doc_delete_job.opensearch.retry import safe_error_summary
from ta_utils.logger.CustomLogger import CustomLogger


class DocumentDeleteJob:
    """Delete the document IDs supplied by the calling service."""

    def __init__(
        self,
        document_delete_handler: DocumentDeleteHandler,
        custom_logger: CustomLogger,
    ) -> None:
        self.document_delete_handler = document_delete_handler
        self.logger = custom_logger

    def run(self, tsp_ids: list[TspId]) -> int:
        """Delete the supplied documents in their received order.

        Input: tsp_ids=[1040, 1041]
        Output: 2 after both documents complete.

        Expected document failures are logged and skipped so later IDs still
        run. The calling service decides when to submit failed IDs again.
        """

        processed_document_count = 0
        failed_tsp_ids: list[TspId] = []
        self.logger.info(
            f"Document Delete job received {len(tsp_ids)} document(s) to process."
        )
        for tsp_id in tsp_ids:
            try:
                self._process_document(tsp_id)
                processed_document_count += 1
            except (OperationFailedError, PermanentDataError, QueueSendError):
                failed_tsp_ids.append(tsp_id)

        if failed_tsp_ids:
            failed_id_list = json.dumps([str(tsp_id) for tsp_id in failed_tsp_ids])
            self.logger.error(
                "Failed document-deletion tsp_id(s) requiring later "
                f"reprocessing: {failed_id_list}"
            )

        self.logger.info(
            f"Document Delete job completed {processed_document_count} of "
            f"{len(tsp_ids)} document(s). Failed document count: "
            f"{len(failed_tsp_ids)}."
        )
        return processed_document_count

    def _process_document(self, tsp_id: TspId) -> None:
        """Delete one document and notify the Case Similarity Backfill queue."""

        self.logger.set_generic_message(f"tsp_id={tsp_id}")
        try:
            self.logger.info("Starting document deletion.")
            self.document_delete_handler.process_document_deletion(tsp_id)
            self.logger.info(
                "Document deletion completed and sent the Case Similarity "
                "Backfill SQS message."
            )
        except (OperationFailedError, PermanentDataError, QueueSendError) as error:
            self.logger.error(f"Document deletion stopped because of {safe_error_summary(error)}")
            raise
        finally:
            self.logger.set_generic_message(None)
