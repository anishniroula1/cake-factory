from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from sqlalchemy.orm import Session

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import (
    CatalogDocument,
    LiveNextQueueMessage,
    LiveResult,
    TspId,
)
from doc2vec_completed.opensearch.db_store import (
    SentenceVectorDBStore,
    TextSimilarityInputDBStore,
)
from doc2vec_completed.opensearch.errors import QueueSendError
from doc2vec_completed.opensearch.open_search_helper import OpenSearchHelper
from doc2vec_completed.opensearch.opensearch_store import (
    SentenceCatalogOpenSearchStore,
    SentenceOccurrenceOpenSearchStore,
)
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.sentence_splitter import split_document
from doc2vec_completed.opensearch.text import sentence_word_count
from ta_db_connection.atables import SentencesVector, TextSimilarityInput
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class LiveHandler:
    """Split and index one newly received document."""

    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch_helper: OpenSearchHelper,
        aws_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.opensearch_helper = opensearch_helper
        self.aws_sqs = aws_sqs
        self.logger = custom_logger
        self.document_db_store = TextSimilarityInputDBStore(session_factory)
        self.sentence_vector_db_store = SentenceVectorDBStore(session_factory)
        self.sentence_catalog_open_search_store = SentenceCatalogOpenSearchStore(
            config,
            opensearch_helper,
        )
        self.sentence_occurrence_open_search_store = SentenceOccurrenceOpenSearchStore(
            config, opensearch_helper
        )

    def process(self, tsp_id: TspId) -> LiveResult | None:
        """Create or reuse sentence rows, then write them to OpenSearch.

        Input: tsp_id=1040
        Output: {"tsp_id": 1040, "source_count": 65,
        "indexed_count": 65, "success": True, "reason": ""}

        A document with no usable text sends success=False and reason=no_content
        so the existing state-machine flow can handle it without retrying SQS.
        """

        started_at = perf_counter()
        self.logger.info("Live sentence processing started.")
        rows = retry_operation(
            "Check for saved live sentences",
            self.sentence_vector_db_store.load_live_sentence_rows,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )

        if rows and all(row.sentence_key for row in rows):
            document = rows[0].tsp
            self.logger.info(
                f"Found {len(rows)} saved sentence(s) with sentence keys. "
                "Skipping sentence splitting and continuing with OpenSearch."
            )
        else:
            document = retry_operation(
                "Load the incoming document",
                self.document_db_store.load_document,
                tsp_id,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
            if document is None:
                self.logger.error(
                    "No text_similarity_input row exists for this document."
                )
                return None

            sentences = self._clean_sentences(split_document(document.content or ""))
            if not sentences:
                retry_operation(
                    "Send the no-content document message",
                    self._publish,
                    tsp_id,
                    False,
                    "no_content",
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
                self.logger.info("The document has no sentence content to index.")
                return {
                    "tsp_id": tsp_id,
                    "source_count": 0,
                    "indexed_count": 0,
                    "success": False,
                    "reason": "no_content",
                }

            rows = self._save_sentences(document, sentences)

        catalog_alias = self.sentence_catalog_open_search_store.select_alias(
            document.analysis_group
        )
        indexed = self._index_sentences(rows)

        retry_operation(
            "Refresh indexed live sentences",
            self.opensearch_helper.refresh_aliases,
            [catalog_alias, constants.SENTENCE_OCCURRENCE_ALIAS],
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        retry_operation(
            "Send the completed live document message",
            self._publish,
            tsp_id,
            True,
            "",
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Live sentence processing completed. Source count: {len(rows)}. "
            f"Indexed count: {indexed}."
        )
        self.logger.info(
            f"Live sentence processing took {perf_counter() - started_at:.2f} "
            "seconds to complete."
        )
        return {
            "tsp_id": tsp_id,
            "source_count": len(rows),
            "indexed_count": indexed,
            "success": True,
            "reason": "",
        }

    @staticmethod
    def _clean_sentences(sentences: list[str]) -> list[str]:
        """Remove empty sentence values and trim surrounding whitespace.

        Input: ["  First sentence.  ", "", "Second sentence."]
        Output: ["First sentence.", "Second sentence."]

        Text inside the sentence is not changed, so its saved content and
        sentence key remain based on the wording received from the document.
        """

        return [cleaned for sentence in sentences if (cleaned := sentence.strip())]

    def _save_sentences(
        self,
        document: TextSimilarityInput,
        sentences: list[str],
    ) -> list[SentencesVector]:
        """Save sentence rows in ordered commits and return their global IDs.

        Input: 2,300 sentence strings with a database batch size of 1,000.
        Output: 2,300 SentencesVector rows saved in three commits.
        """

        numbered_sentences = list(enumerate(sentences, start=1))
        saved_rows: list[SentencesVector] = []
        for batch_number, sentence_batch in enumerate(
            batches(numbered_sentences, constants.LIVE_DATABASE_BATCH_SIZE),
            start=1,
        ):
            rows = retry_operation(
                f"Save live sentence batch {batch_number}",
                self.sentence_vector_db_store.save_live_sentence_batch,
                document,
                sentence_batch,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
            saved_rows.extend(rows)
            self.logger.info(
                f"Saved live sentence batch {batch_number} with {len(rows)} "
                "sentence(s)."
            )
        return saved_rows

    def _index_sentences(self, rows: list[SentencesVector]) -> int:
        """Index saved sentence rows in concurrent worker batches.

        Input: 65 rows and worker_batch_size=30
        Output: 65 after three worker batches finish.
        """

        worker_batches = list(batches(rows, constants.WORKER_BATCH_SIZE))
        self.logger.info(
            f"Indexing {len(rows)} live sentence(s) in "
            f"{len(worker_batches)} worker batch(es)."
        )
        with ThreadPoolExecutor(
            max_workers=constants.THREAD_WORKER_COUNT,
            thread_name_prefix="live-sentence",
        ) as executor:
            return sum(executor.map(self._run_worker, worker_batches))

    def _run_worker(self, rows: list[SentencesVector]) -> int:
        """Write one live batch without regenerating keys or reading DB vectors.

        Every row already contains its saved sentence key. Missing catalog
        documents are sent through the Bedrock pipeline, and every row is then
        written to the occurrence index.
        """

        label = (
            f"Live worker batch for global IDs {rows[0].global_id} "
            f"through {rows[-1].global_id}"
        )
        document = rows[0].tsp
        catalog_alias = self.sentence_catalog_open_search_store.select_alias(
            document.analysis_group
        )
        sentence_keys = [row.sentence_key for row in rows]
        existing_keys = retry_operation(
            f"Check existing catalog keys for {label}",
            self.sentence_catalog_open_search_store.find_existing_sentence_keys,
            catalog_alias,
            sentence_keys,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        catalog_documents = self._missing_catalog_documents(rows, existing_keys)
        retry_operation(
            f"Create Bedrock vectors in {catalog_alias} for {label}",
            self.sentence_catalog_open_search_store.index_with_bedrock,
            catalog_alias,
            catalog_documents,
            wait_seconds=constants.RETRY_SECONDS,
            max_attempts=constants.OPENSEARCH_BULK_RETRY_COUNT + 1,
            custom_logger=self.logger,
        )

        occurrence_documents = (
            self.sentence_occurrence_open_search_store.build_documents(rows)
        )
        retry_operation(
            f"Write occurrences for {label}",
            self.sentence_occurrence_open_search_store.index_occurrences,
            occurrence_documents,
            wait_seconds=constants.RETRY_SECONDS,
            max_attempts=constants.OPENSEARCH_BULK_RETRY_COUNT + 1,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"{label} completed: {len(existing_keys)} existing catalog key(s), "
            f"{len(catalog_documents)} Bedrock vector(s), and "
            f"{len(occurrence_documents)} occurrence(s)."
        )
        return len(rows)

    def _missing_catalog_documents(
        self,
        rows: list[SentencesVector],
        existing_keys: set[str],
    ) -> list[CatalogDocument]:
        """Build one catalog document for each unique key not in OpenSearch."""

        documents: list[CatalogDocument] = []
        seen_keys: set[str] = set()
        for row in rows:
            sentence_key = row.sentence_key
            if sentence_key in existing_keys or sentence_key in seen_keys:
                continue
            seen_keys.add(sentence_key)
            documents.append(
                {
                    "_id": sentence_key,
                    "sentenceKey": sentence_key,
                    "sentenceContent": row.sentence_content,
                    "wordCount": sentence_word_count(row.sentence_content),
                    "createdAt": row.last_modified.isoformat(),
                    "updatedAt": row.last_modified.isoformat(),
                }
            )
        return documents

    def _publish(self, tsp_id: TspId, success: bool, reason: str) -> None:
        """Send the existing Doc2Vec result contract to the state machine.

        Input: tsp_id=1040, success=True, reason=""
        Output message: {"source": "P2", "tsp_id": 1040,
        "sent_by": "doc2vec", "success": true, "reason": ""}
        """

        queue_url = self.config.state_machine_queue_url
        if not queue_url:
            raise QueueSendError("State Machine SQS queue URL is not configured")

        message: LiveNextQueueMessage = {
            "source": "P2",
            "tsp_id": tsp_id,
            "sent_by": "doc2vec",
            "success": success,
            "reason": reason,
        }
        sent = self.aws_sqs.send_sqs_message(
            send_queue_url=queue_url,
            message_body=message,
            is_fifo=queue_url.endswith(".fifo"),
        )
        if not sent:
            raise QueueSendError("Failed to send the Doc2Vec state-machine message")
