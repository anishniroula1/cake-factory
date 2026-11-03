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
        case_similarity_backfill_sqs: AwsSQS,
        case_similarity_backfill_queue_url: str,
        custom_logger: CustomLogger,
        database_delete_enabled: bool,
    ) -> None:
        self.sentence_occurrence_open_search_store = sentence_occurrence_open_search_store
        self.sentence_catalog_open_search_store = sentence_catalog_open_search_store
        self.sentence_vector_db_store = sentence_vector_db_store
        self.embeddings_db_store = embeddings_db_store
        self.titan_matches_db_store = titan_matches_db_store
        self.text_similarity_input_db_store = text_similarity_input_db_store
        self.case_similarity_backfill_sqs = case_similarity_backfill_sqs
        self.case_similarity_backfill_queue_url = case_similarity_backfill_queue_url
        self.logger = custom_logger
        self.database_delete_enabled = database_delete_enabled

    def process_document_deletion(self, tsp_id: TspId) -> DeleteResult:
        """Delete OpenSearch and Titan data, then optionally delete legacy data.

        Input: tsp_id=8205
        Output: the main deletion counts for the document.

        A Case Similarity Backfill SQS message is sent only after every enabled
        deletion step finishes. The calling service owns document-state management.
        """

        started_at = perf_counter()
        self.logger.info(
            "Starting Amazon OpenSearch deletion from sentence occurrence and "
            "sentence catalog indexes."
        )
        opensearch_deletion_result = self._delete_document_from_opensearch(tsp_id)
        self.logger.info(
            "Amazon OpenSearch deletion completed. Removed "
            f"{opensearch_deletion_result.occurrence_count} occurrence(s) and "
            f"{opensearch_deletion_result.catalog_count} orphaned catalog vector(s)."
        )

        self.logger.info(
            "Starting PostgreSQL bidirectional deletion from tsp.titan_matches."
        )
        titan_match_deletion_result = self._delete_titan_matches(tsp_id)
        self.logger.info(
            "PostgreSQL tsp.titan_matches deletion completed. Removed "
            f"{titan_match_deletion_result.deleted_match_bucket_count} source "
            "bucket(s)."
        )
        database_deletion_result = titan_match_deletion_result
        # OpenSearch and Titan cleanup always run. This flag additionally gates
        # legacy embedding and sentence-vector cleanup.
        if self.database_delete_enabled:
            self.logger.info(
                "Starting enabled PostgreSQL cleanup for tsp.embeddings and "
                "tsp.sentences_vector."
            )
            legacy_deletion_result = self._delete_legacy_document_data(tsp_id)
            database_deletion_result = DatabaseDeletionResult(
                affected_application_ids=titan_match_deletion_result.affected_application_ids,
                affected_tsp_ids=titan_match_deletion_result.affected_tsp_ids,
                has_deleted_document=titan_match_deletion_result.has_deleted_document,
                deleted_match_bucket_count=titan_match_deletion_result.deleted_match_bucket_count,
                deleted_embedding_count=legacy_deletion_result.deleted_embedding_count,
                deleted_sentence_count=legacy_deletion_result.deleted_sentence_count,
            )
            self._update_document_state(tsp_id)
            self.logger.info(
                "Enabled PostgreSQL cleanup completed. Removed "
                f"{database_deletion_result.deleted_embedding_count} embedding(s) "
                f"and {database_deletion_result.deleted_sentence_count} "
                "sentence-vector row(s), then updated tsp.text_similarity_input."
            )
        else:
            self.logger.info(
                "Legacy database deletion is disabled. Titan matches were cleaned, "
                "but embeddings and sentence-vector rows were not changed."
            )

        # Case Similarity must refresh its application-level result after all
        # OpenSearch and enabled database deletion work has completed.
        self._send_to_case_similarity_backfill_queue(tsp_id)
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
            "affected_application_ids": database_deletion_result.affected_application_ids,
            "affected_tsp_ids": database_deletion_result.affected_tsp_ids,
            "has_deleted_document": database_deletion_result.has_deleted_document,
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
        self.logger.info(
            "Loading document occurrences from Amazon OpenSearch alias "
            f"{constants.SENTENCE_OCCURRENCE_ALIAS}."
        )
        occurrence_records = retry_operation(
            "Load sentence occurrences",
            self.sentence_occurrence_open_search_store.load_document_occurrences,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        self.logger.info(
            f"Loaded {len(occurrence_records)} occurrence record(s) from "
            f"{constants.SENTENCE_OCCURRENCE_ALIAS}."
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
            self.logger.info(
                f"Checking {len(sentence_keys)} unique sentence key(s) for "
                f"remaining {analysis_group} occurrence references in Amazon OpenSearch."
            )
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
            self.logger.info(
                f"Amazon OpenSearch reference check completed. "
                f"{len(shared_sentence_keys)} sentence key(s) are still shared."
            )
            # The set difference is the catalog delete list. Shared keys stay;
            # keys referenced only by this tsp_id are orphaned and are removed.
            orphaned_sentence_keys = [
                sentence_key for sentence_key in sentence_keys if sentence_key not in shared_sentence_keys
            ]
            if orphaned_sentence_keys:
                self.logger.info(
                    f"Deleting {len(orphaned_sentence_keys)} orphaned vector(s) "
                    f"from the {analysis_group} sentence catalog."
                )
                deleted_catalog_count = retry_operation(
                    "Delete unreferenced catalog vectors",
                    self.sentence_catalog_open_search_store.delete_catalog_documents_by_sentence_key,
                    analysis_group,
                    orphaned_sentence_keys,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                    max_attempts=constants.MAX_ATTEMPTS,
                )
                self.logger.info(
                    f"Sentence catalog deletion completed for "
                    f"{deleted_catalog_count} vector(s)."
                )

        # Delete all occurrences for the tsp_id together, including occurrences
        # whose shared sentenceKey caused their catalog vector to be kept. This
        # runs last so catalog failures leave the reference data retryable.
        self.logger.info(
            f"Deleting document occurrences from Amazon OpenSearch alias "
            f"{constants.SENTENCE_OCCURRENCE_ALIAS}."
        )
        deleted_occurrence_count = retry_operation(
            "Delete sentence occurrences",
            self.sentence_occurrence_open_search_store.delete_occurrences_by_tsp_id,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        self.logger.info(
            f"Occurrence deletion completed for {deleted_occurrence_count} record(s)."
        )

        return OpenSearchDeletionResult(
            occurrence_count=deleted_occurrence_count,
            catalog_count=deleted_catalog_count,
        )

    def _delete_titan_matches(self, tsp_id: TspId) -> DatabaseDeletionResult:
        """Delete source and reverse Titan relationships for one document.

        Input: tsp_id=8205
        Output: affected application/document IDs, a found flag, and the
        deleted source-bucket count.

        This cleanup always runs because titan_matches belongs to the new
        OpenSearch flow and is not controlled by the legacy database toggle.
        """

        self.logger.info(
            "Calling PostgreSQL function tsp.delete_titan_matches_for_tsp."
        )
        deletion_result = retry_operation(
            "Delete Titan matches",
            self.titan_matches_db_store.delete_bidirectional_document_matches,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        self.logger.info(
            "PostgreSQL function tsp.delete_titan_matches_for_tsp completed. "
            f"Affected application count: {len(deletion_result.affected_application_ids)}. "
            f"Affected tsp_id count: {len(deletion_result.affected_tsp_ids)}."
        )
        if not deletion_result.has_deleted_document:
            self.logger.info("No titan_matches rows were found for this document.")
        return DatabaseDeletionResult(
            affected_application_ids=deletion_result.affected_application_ids,
            affected_tsp_ids=deletion_result.affected_tsp_ids,
            has_deleted_document=deletion_result.has_deleted_document,
            deleted_match_bucket_count=deletion_result.deleted_match_bucket_count,
        )

    def _delete_legacy_document_data(self, tsp_id: TspId) -> DatabaseDeletionResult:
        """Delete legacy embeddings and sentence rows when the toggle is enabled.

        Input: tsp_id=8205
        Output: deleted embedding and sentence-row counts.
        """

        self.logger.info(
            "Loading document global IDs from PostgreSQL table tsp.sentences_vector."
        )
        sentence_vector_context = retry_operation(
            "Load document sentences",
            self.sentence_vector_db_store.load_document_sentence_context,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        self.logger.info(
            f"Loaded {len(sentence_vector_context.global_ids)} global ID(s) from "
            "tsp.sentences_vector."
        )
        self.logger.info("Deleting document vectors from PostgreSQL table tsp.embeddings.")
        deleted_embedding_count = retry_operation(
            "Delete sentence embeddings",
            self.embeddings_db_store.delete_sentence_embeddings,
            sentence_vector_context.global_ids,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        self.logger.info(
            f"PostgreSQL tsp.embeddings deletion completed for "
            f"{deleted_embedding_count} row(s)."
        )
        self.logger.info(
            "Deleting document sentences from PostgreSQL table tsp.sentences_vector."
        )
        deleted_sentence_count = retry_operation(
            "Delete sentence vector rows",
            self.sentence_vector_db_store.delete_document_sentence_records,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        self.logger.info(
            f"PostgreSQL tsp.sentences_vector deletion completed for "
            f"{deleted_sentence_count} row(s)."
        )
        return DatabaseDeletionResult(
            deleted_embedding_count=deleted_embedding_count,
            deleted_sentence_count=deleted_sentence_count,
        )

    def _send_to_case_similarity_backfill_queue(self, tsp_id: TspId) -> None:
        """Send the deleted tsp_id to Case Similarity Backfill for refresh."""

        message_sent = self.case_similarity_backfill_sqs.send_sqs_message(
            send_queue_url=self.case_similarity_backfill_queue_url,
            message_body={"source": "P2", "tsp_id": str(tsp_id), "sent_by": "doc-delete"},
            wait_time=constants.RETRY_SECONDS,
            retries=constants.MAX_ATTEMPTS - 1,
        )
        if not message_sent:
            raise QueueSendError(
                "failed to send the deleted document to the Case Similarity Backfill queue"
            )

    def _update_document_state(self, tsp_id: TspId) -> None:
        """Set the retained TSI record to DOC_DELETED without reading its state."""

        self.logger.info(
            "Updating document_state in PostgreSQL table tsp.text_similarity_input "
            "to DOC_DELETED."
        )
        updated_count = retry_operation(
            "Update document state to DOC_DELETED",
            self.text_similarity_input_db_store.update_document_state,
            tsp_id,
            constants.DELETED_DOCUMENT_STATE,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        if updated_count != 1:
            raise PermanentDataError(
                f"Expected one text_similarity_input record for tsp_id={tsp_id}, "
                f"updated {updated_count}."
            )
        self.logger.info(
            "PostgreSQL tsp.text_similarity_input document_state update completed."
        )
