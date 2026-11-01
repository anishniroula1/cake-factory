from time import perf_counter

from doc_delete_job.opensearch import constants
from doc_delete_job.opensearch.contracts import DatabaseDeletionResult, DeleteResult, OpenSearchDeletionResult, TspId
from doc_delete_job.opensearch.db_store.embeddings_db_store import EmbeddingsDBStore
from doc_delete_job.opensearch.db_store.sentence_vector_db_store import SentenceVectorDBStore
from doc_delete_job.opensearch.db_store.text_similarity_input_db_store import TextSimilarityInputDBStore
from doc_delete_job.opensearch.db_store.titan_matches_db_store import TitanMatchesDBStore
from doc_delete_job.opensearch.errors import PermanentDataError, QueueSendError
from doc_delete_job.opensearch.opensearch_store.sentence_catalog_open_search_store import SentenceCatalogOpenSearchStore
from doc_delete_job.opensearch.opensearch_store.sentence_occurrence_open_search_store import (
    SentenceOccurrenceOpenSearchStore,
)
from doc_delete_job.opensearch.retry import retry_operation
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class DocumentDeleteHandler:
    """Delete one document from OpenSearch and its Titan match relationships.

    Titan match cleanup always runs because titan_matches belongs to the new
    OpenSearch flow. Legacy embedding and sentence cleanup uses the database
    deletion toggle.
    """

    def __init__(
        self,
        sentence_occurrence_open_search_store: SentenceOccurrenceOpenSearchStore,
        sentence_catalog_open_search_store: SentenceCatalogOpenSearchStore,
        sentence_vector_db_store: SentenceVectorDBStore,
        embeddings_db_store: EmbeddingsDBStore,
        titan_matches_db_store: TitanMatchesDBStore,
        text_similarity_input_db_store: TextSimilarityInputDBStore,
        outbound_sqs: AwsSQS,
        send_sqs_endpoint: str,
        custom_logger: CustomLogger,
        database_delete_enabled: bool,
    ) -> None:
        self.sentence_occurrence_open_search_store = sentence_occurrence_open_search_store
        self.sentence_catalog_open_search_store = sentence_catalog_open_search_store
        self.sentence_vector_db_store = sentence_vector_db_store
        self.embeddings_db_store = embeddings_db_store
        self.titan_matches_db_store = titan_matches_db_store
        self.text_similarity_input_db_store = text_similarity_input_db_store
        self.outbound_sqs = outbound_sqs
        self.send_sqs_endpoint = send_sqs_endpoint
        self.logger = custom_logger
        self.database_delete_enabled = database_delete_enabled

    def process_document_deletion(self, tsp_id: TspId) -> DeleteResult:
        """Delete OpenSearch and Titan data, then optionally delete legacy data.

        Input: tsp_id=8205
        Output: the main deletion counts for the document.

        The next SQS message is sent only after every enabled deletion step
        finishes. A failure leaves the document state ready for the next job.
        """

        started_at = perf_counter()
        opensearch_deletion_result = self._delete_document_from_opensearch(tsp_id)
        titan_match_deletion_result = self._delete_titan_matches(tsp_id)
        database_deletion_result = titan_match_deletion_result
        # OpenSearch and Titan cleanup always run. This flag additionally gates
        # legacy embedding/sentence cleanup and TSI state changes.
        if self.database_delete_enabled:
            legacy_deletion_result = self._delete_legacy_document_data(tsp_id)
            database_deletion_result = DatabaseDeletionResult(
                deleted_match_bucket_count=titan_match_deletion_result.deleted_match_bucket_count,
                deleted_embedding_count=legacy_deletion_result.deleted_embedding_count,
                deleted_sentence_count=legacy_deletion_result.deleted_sentence_count,
            )
            self._update_document_state(tsp_id, constants.DELETED_DOCUMENT_STATE)
        else:
            self.logger.info(
                "Legacy database deletion is disabled. Titan matches were cleaned, "
                "but embeddings and sentence-vector rows were not changed."
            )

        try:
            self._send_deletion_result_to_next_queue(tsp_id)
        except QueueSendError:
            if self.database_delete_enabled:
                self._update_document_state(tsp_id, constants.READY_FOR_DELETION_DOCUMENT_STATE)
            raise
        self.logger.info(
            f"Document deletion removed {opensearch_deletion_result.occurrence_count} occurrence(s), "
            f"{opensearch_deletion_result.catalog_count} orphaned catalog vector(s), "
            f"{database_deletion_result.deleted_match_bucket_count} Titan match bucket(s), "
            f"{database_deletion_result.deleted_embedding_count} embedding(s), and "
            f"{database_deletion_result.deleted_sentence_count} sentence row(s)."
        )
        self.logger.info(f"Document deletion took {perf_counter() - started_at:.2f} seconds to complete.")
        return {
            "tsp_id": tsp_id,
            "occurrence_count": opensearch_deletion_result.occurrence_count,
            "catalog_count": opensearch_deletion_result.catalog_count,
            "deleted_match_bucket_count": database_deletion_result.deleted_match_bucket_count,
            "deleted_embedding_count": database_deletion_result.deleted_embedding_count,
            "deleted_sentence_count": database_deletion_result.deleted_sentence_count,
        }

    def _delete_document_from_opensearch(self, tsp_id: TspId) -> OpenSearchDeletionResult:
        """Delete one document from OpenSearch without deleting shared vectors.
        from different tsp_id with same sentence_key.

        The method first reads the document's sentence occurrences to get its
        analysis group and sentence keys. It then checks whether
        another tsp_id in the same analysis group uses each sentence key:

        * Keep the catalog vector when another tsp_id in the same group uses it.
        * Delete the catalog vector when only the tsp_id being deleted uses it.
        * Do not check or change another analysis group's catalog. Each analysis
          group has its own catalog, even when both catalogs contain the same key.

        Example before deleting tsp_id 100:

        * Asylee tsp_id 100 uses key-a and key-b.
        * Asylee tsp_id 200 also uses key-b.
        * Refugee tsp_id 300 uses key-a.

        After deleting tsp_id 100:

        * All occurrence records for tsp_id 100 are deleted.
        * key-b stays in the Asylee catalog because Asylee tsp_id 200 uses it.
        * key-a is deleted from the Asylee catalog because no other Asylee tsp_id
          uses it.
        * key-a stays in the Refugee catalog. The Refugee catalog is separate and
          is not changed while an Asylee document is being deleted.

        Catalog vectors are handled before the occurrences are deleted. If the
        catalog operation fails, the occurrence records are still available for
        the next retry.
        """

        # This snapshot supplies sentenceKey values for catalog ownership checks.
        # Occurrence deletion itself uses one tspId delete-by-query operation.
        occurrence_records = retry_operation(
            "Load sentence occurrences",
            self.sentence_occurrence_open_search_store.load_document_occurrences,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        # A tsp_id belongs to one analysis group. Catalog aliases are separated
        # by group, so a reference in the other group must not keep this vector.
        analysis_group = occurrence_records[0]["analysis_group"] if occurrence_records else None

        # dict.fromkeys removes duplicate sentence keys while preserving their order.
        # A repeated sentence key must be checked and deleted only once.
        sentence_keys = list(
            dict.fromkeys(occurrence_record["sentence_key"] for occurrence_record in occurrence_records)
        )
        shared_sentence_keys: set[str] = set()
        deleted_catalog_count = 0
        if analysis_group and sentence_keys:
            # Excluding the current tsp_id answers the exact ownership question:
            # which keys will still have a same-group occurrence after this delete?
            shared_sentence_keys = retry_operation(
                "Check remaining sentence-key references",
                self.sentence_occurrence_open_search_store.find_sentence_keys_used_by_other_documents,
                sentence_keys,
                analysis_group,
                tsp_id,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
                max_attempts=constants.MAX_ATTEMPTS,
            )
            # The set difference is the catalog delete list. Shared keys stay;
            # keys referenced only by this tsp_id are orphaned and are removed.
            orphaned_sentence_keys = [
                sentence_key for sentence_key in sentence_keys if sentence_key not in shared_sentence_keys
            ]
            if orphaned_sentence_keys:
                deleted_catalog_count = retry_operation(
                    "Delete unreferenced catalog vectors",
                    self.sentence_catalog_open_search_store.delete_catalog_documents_by_sentence_key,
                    analysis_group,
                    orphaned_sentence_keys,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                    max_attempts=constants.MAX_ATTEMPTS,
                )

        # Delete all occurrences for the tsp_id together, including occurrences
        # whose shared sentenceKey caused their catalog vector to be kept. This
        # runs last so catalog failures leave the reference data retryable.
        deleted_occurrence_count = retry_operation(
            "Delete sentence occurrences",
            self.sentence_occurrence_open_search_store.delete_occurrences_by_tsp_id,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )

        return OpenSearchDeletionResult(
            occurrence_count=deleted_occurrence_count,
            catalog_count=deleted_catalog_count,
        )

    def _delete_titan_matches(self, tsp_id: TspId) -> DatabaseDeletionResult:
        """Delete source and reverse Titan relationships for one document.

        Input: tsp_id=8205
        Output: deleted source-bucket and updated reverse-bucket counts.

        This cleanup always runs because titan_matches belongs to the new
        OpenSearch flow and is not controlled by the legacy database toggle.
        """

        deletion_result = retry_operation(
            "Delete Titan matches",
            self.titan_matches_db_store.delete_bidirectional_document_matches,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        return DatabaseDeletionResult(
            deleted_match_bucket_count=deletion_result.deleted_match_bucket_count,
        )

    def _delete_legacy_document_data(self, tsp_id: TspId) -> DatabaseDeletionResult:
        """Delete legacy embeddings and sentence rows when the toggle is enabled.

        Input: tsp_id=8205
        Output: deleted embedding and sentence-row counts.
        """

        sentence_vector_context = retry_operation(
            "Load document sentences",
            self.sentence_vector_db_store.load_document_sentence_context,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        deleted_embedding_count = retry_operation(
            "Delete sentence embeddings",
            self.embeddings_db_store.delete_sentence_embeddings,
            sentence_vector_context.global_ids,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        deleted_sentence_count = retry_operation(
            "Delete sentence vector rows",
            self.sentence_vector_db_store.delete_document_sentence_records,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        return DatabaseDeletionResult(
            deleted_embedding_count=deleted_embedding_count,
            deleted_sentence_count=deleted_sentence_count,
        )

    def _send_deletion_result_to_next_queue(self, tsp_id: TspId) -> None:
        """Send the tsp_id after deletion so Case Similarity can refresh the application."""

        message_sent = self.outbound_sqs.send_sqs_message(
            send_queue_url=self.send_sqs_endpoint,
            message_body={"source": "P2", "tsp_id": str(tsp_id), "sent_by": "doc-delete"},
            wait_time=constants.RETRY_SECONDS,
            retries=constants.MAX_ATTEMPTS - 1,
        )
        if not message_sent:
            raise QueueSendError("failed to send the document to the next SQS queue")

    def _update_document_state(self, tsp_id: TspId, document_state: str) -> None:
        """Persist the document state and require exactly one retained TSI row."""

        updated_count = retry_operation(
            f"Update document state to {document_state}",
            self.text_similarity_input_db_store.update_document_state,
            tsp_id,
            document_state,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        if updated_count != 1:
            raise PermanentDataError(
                f"Expected one tsp.text_similarity_input row for tsp_id={tsp_id}, updated {updated_count}."
            )
