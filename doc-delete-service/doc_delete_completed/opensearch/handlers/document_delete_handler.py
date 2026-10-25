from time import perf_counter

from doc_delete_completed.opensearch import constants
from doc_delete_completed.opensearch.contracts import (
    DatabaseDeletionResult,
    DeleteResult,
    OpenSearchDeletionResult,
    TspId,
)
from doc_delete_completed.opensearch.db_store.embeddings_db_store import EmbeddingsDBStore
from doc_delete_completed.opensearch.db_store.sentence_vector_db_store import SentenceVectorDBStore
from doc_delete_completed.opensearch.db_store.titan_matches_db_store import TitanMatchesDBStore
from doc_delete_completed.opensearch.errors import QueueSendError
from doc_delete_completed.opensearch.opensearch_store.sentence_catalog_open_search_store import (
    SentenceCatalogOpenSearchStore,
)
from doc_delete_completed.opensearch.opensearch_store.sentence_occurrence_open_search_store import (
    SentenceOccurrenceOpenSearchStore,
)
from doc_delete_completed.opensearch.retry import retry_operation
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class DocumentDeleteHandler:
    """Delete one document from OpenSearch and optionally from PostgreSQL.

    OpenSearch cleanup always runs. PostgreSQL cleanup runs only when the
    database deletion toggle is enabled.
    """

    def __init__(
        self,
        sentence_occurrence_open_search_store: SentenceOccurrenceOpenSearchStore,
        sentence_catalog_open_search_store: SentenceCatalogOpenSearchStore,
        sentence_vector_db_store: SentenceVectorDBStore,
        embeddings_db_store: EmbeddingsDBStore,
        titan_matches_db_store: TitanMatchesDBStore,
        aws_sqs: AwsSQS,
        sent_sqs_endpoint: str,
        custom_logger: CustomLogger,
        database_delete_enabled: bool,
    ) -> None:
        self.sentence_occurrence_open_search_store = sentence_occurrence_open_search_store
        self.sentence_catalog_open_search_store = sentence_catalog_open_search_store
        self.sentence_vector_db_store = sentence_vector_db_store
        self.embeddings_db_store = embeddings_db_store
        self.titan_matches_db_store = titan_matches_db_store
        self.aws_sqs = aws_sqs
        self.sent_sqs_endpoint = sent_sqs_endpoint
        self.logger = custom_logger
        self.database_delete_enabled = database_delete_enabled

    def process_document_deletion(self, tsp_id: TspId) -> DeleteResult:
        """Run OpenSearch deletion and run database deletion only when enabled.

        Input: tsp_id=8205
        Output: deletion counts, toggle state, and next-message confirmation.

        The next SQS message is sent only after every enabled deletion step
        finishes. A failure leaves the source message available for SQS retry.
        """

        started_at = perf_counter()
        opensearch_deletion_result = self._delete_document_from_opensearch(tsp_id)
        database_deletion_result = DatabaseDeletionResult()
        if self.database_delete_enabled:
            database_deletion_result = self._delete_document_from_database(
                tsp_id, opensearch_deletion_result.global_ids
            )
        else:
            self.logger.info("Database deletion is disabled. PostgreSQL data was not changed.")

        self._send_deletion_result_to_next_queue(tsp_id)
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
            "shared_catalog_count": opensearch_deletion_result.shared_catalog_count,
            "deleted_match_bucket_count": database_deletion_result.deleted_match_bucket_count,
            "updated_reverse_bucket_count": database_deletion_result.updated_reverse_bucket_count,
            "deleted_embedding_count": database_deletion_result.deleted_embedding_count,
            "deleted_sentence_count": database_deletion_result.deleted_sentence_count,
            "database_delete_enabled": self.database_delete_enabled,
            "next_message_sent": True,
        }

    def _delete_document_from_opensearch(self, tsp_id: TspId) -> OpenSearchDeletionResult:
        """Delete orphaned catalog vectors, then delete document occurrences.

        Input: tsp_id=8205
        Output: OpenSearch counts plus global IDs for optional database cleanup.

        A catalog vector is shared by every occurrence with the same sentence
        key. It is deleted only when no other tsp_id in the analysis group uses
        that key. Occurrences are deleted last so a catalog failure can be
        retried using the original occurrence records.
        """

        occurrence_records = retry_operation(
            "Load sentence occurrences",
            self.sentence_occurrence_open_search_store.load_document_occurrences,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        analysis_group = occurrence_records[0]["analysis_group"] if occurrence_records else None

        # dict.fromkeys removes duplicates while keeping the original order.
        # A repeated sentence key must be checked and deleted only once.
        global_ids = list(dict.fromkeys(occurrence_record["global_id"] for occurrence_record in occurrence_records))
        sentence_keys = list(
            dict.fromkeys(occurrence_record["sentence_key"] for occurrence_record in occurrence_records)
        )
        shared_sentence_keys: set[str] = set()
        deleted_catalog_count = 0
        if analysis_group and sentence_keys:
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
            # Example: if key-a is used only by this document but another
            # document uses key-b, delete key-a and keep key-b.
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

        # Occurrences are deleted last. If catalog cleanup fails, the original
        # occurrence records remain available and SQS can retry the full flow.
        if global_ids:
            retry_operation(
                "Delete sentence occurrences",
                self.sentence_occurrence_open_search_store.delete_occurrences_by_global_id,
                global_ids,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
                max_attempts=constants.MAX_ATTEMPTS,
            )

        return OpenSearchDeletionResult(
            occurrence_count=len(global_ids),
            catalog_count=deleted_catalog_count,
            shared_catalog_count=len(shared_sentence_keys),
            global_ids=global_ids,
        )

    def _delete_document_from_database(self, tsp_id: TspId, opensearch_global_ids: list[int]) -> DatabaseDeletionResult:
        """Delete Titan matches, embeddings, and sentence rows when the toggle is enabled.

        Input: tsp_id=8205, global_ids=[100, 101]
        Output: counts for deleted or updated PostgreSQL rows.

        Titan relationships are removed before their sentence rows. This lets
        the database function find the source IDs and remove reverse matches
        owned by other documents.
        """

        sentence_vector_context = retry_operation(
            "Load document sentences",
            self.sentence_vector_db_store.load_document_sentence_context,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        # Use IDs found in either system. For example, if sentences_vector was
        # already cleaned but OpenSearch still returned ID 100, its embedding
        # can still be deleted.
        embedding_global_ids = list(dict.fromkeys(sentence_vector_context.global_ids + opensearch_global_ids))
        titan_match_deletion_result = retry_operation(
            "Delete Titan matches",
            self.titan_matches_db_store.delete_bidirectional_document_matches,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
            max_attempts=constants.MAX_ATTEMPTS,
        )
        deleted_embedding_count = retry_operation(
            "Delete sentence embeddings",
            self.embeddings_db_store.delete_sentence_embeddings,
            embedding_global_ids,
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
            deleted_match_bucket_count=titan_match_deletion_result.deleted_match_bucket_count,
            updated_reverse_bucket_count=titan_match_deletion_result.updated_reverse_bucket_count,
            deleted_embedding_count=deleted_embedding_count,
            deleted_sentence_count=deleted_sentence_count,
        )

    def _send_deletion_result_to_next_queue(self, tsp_id: TspId) -> None:
        """Send the tsp_id after deletion so Case Similarity can refresh the application."""

        message_sent = self.aws_sqs.send_sqs_message(
            send_queue_url=self.sent_sqs_endpoint,
            message_body={"source": "P2", "tsp_id": str(tsp_id), "sent_by": "doc-delete"},
            wait_time=constants.RETRY_SECONDS,
            retries=constants.MAX_ATTEMPTS - 1,
        )
        if not message_sent:
            raise QueueSendError("failed to send the document to the next SQS queue")
