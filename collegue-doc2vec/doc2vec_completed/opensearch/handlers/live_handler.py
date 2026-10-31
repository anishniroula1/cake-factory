import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from sqlalchemy.orm import Session

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import (
    LiveNextQueueMessage,
    LiveResult,
    TspId,
)
from doc2vec_completed.opensearch.db_store import (
    SentenceVectorDBStore,
    TextSimilarityInputDBStore,
)
from doc2vec_completed.opensearch.errors import QueueSendError
from doc2vec_completed.opensearch.opensearch_helper import OpenSearchHelper
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.rrf_filterer import RRFFilterer
from doc2vec_completed.opensearch.sentence_batch_processor import (
    SentenceBatchProcessor,
)
from doc2vec_completed.opensearch.sentence_splitter import split_document
from ta_db_connection.atatables import SentencesVector, TextSimilarityInput
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
        self.text_similarity_input_db_store = TextSimilarityInputDBStore(session_factory)
        self.sentence_vector_db_store = SentenceVectorDBStore(session_factory)
        self.rrf_filterer = RRFFilterer(custom_logger)
        self.batch_processor = SentenceBatchProcessor(
            opensearch_helper,
            custom_logger,
        )

    def process(self, tsp_id: TspId) -> LiveResult | None:
        """Process one document and save its sentences in PostgreSQL and OpenSearch.

        The flow first looks for existing sentences_vector records for the
        supplied tsp_id. Existing records are reused and any missing sentence
        keys are generated, allowing a retried message to continue without
        splitting or inserting the document again.

        When no saved sentences exist, the document is loaded from
        text_similarity_input, filtered as needed, split with the existing
        Syntok logic, and saved in sentences_vector with its sentence keys and
        generated global IDs.

        Every saved sentence is then written to the sentence occurrence index.
        Each unique sentence key is also written to the catalog selected by the
        document's analysis group, where OpenSearch creates any missing Titan
        embedding.

        After all database and OpenSearch work succeeds, the indexes are
        refreshed and the processing result is sent to the next SQS queue.
        Errors are logged by the operation that failed so SQS can retry the
        source message without creating duplicate records.
        """

        started_at = perf_counter()
        self.logger.info("Live Sentence processing started.")
        sentence_vector_records = self._load_saved_document_sentences(tsp_id)

        if sentence_vector_records:
            generated_key_count, reused_key_count = self._prepare_saved_sentence_keys(
                sentence_vector_records
            )
            document = sentence_vector_records[0].tsp
            self._log_document_context(document)
            self.logger.info(
                f"Found {len(sentence_vector_records)} saved sentence(s). "
                f"Generated {generated_key_count} missing sentence key(s) and "
                f"reused {reused_key_count} existing key(s). Skipping sentence "
                "splitting and continuing with OpenSearch."
            )
        else:
            document, sentence_vector_records, completed_result = (
                self._load_and_save_new_document_sentences(tsp_id, started_at)
            )
            if completed_result is not None:
                return completed_result
            if document is None:
                return None

        catalog_alias = self.batch_processor.select_catalog_alias(
            document.analysis_group
        )
        indexed = self._save_sentences_in_opensearch(sentence_vector_records)

        # Bulk writes can finish before OpenSearch's automatic refresh runs.
        # Refresh both aliases once so Sentence Similarity can immediately find
        # the new occurrence records and their catalog vectors.
        retry_operation(
            "Refresh indexed sentences",
            self.opensearch_helper.refresh_aliases,
            [catalog_alias, constants.SENTENCE_OCCURRENCE_ALIAS],
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        result = self._send_processing_result(
            tsp_id=tsp_id,
            source_count=len(sentence_vector_records),
            indexed_count=indexed,
            success=True,
            reason="",
        )
        self.logger.info(
            f"Sentence processing completed. Source count: "
            f"{len(sentence_vector_records)}. "
            f"Indexed count: {indexed}."
        )
        self.logger.info(
            f"Sentence processing took {perf_counter() - started_at:.2f} "
            "seconds to complete."
        )
        return result

    def _load_and_save_new_document_sentences(
        self,
        tsp_id: TspId,
        started_at: float,
    ) -> tuple[
        TextSimilarityInput | None,
        list[SentencesVector],
        LiveResult | None,
    ]:
        """Load, split, and save sentences when no reusable records were found.

        Input: tsp_id=1040 and the processing start time.
        Output: (document, saved sentence records, None) after a successful save.

        A missing document returns (None, [], None). A document without usable
        sentences sends its no_content result and returns it in the last value.
        """

        document = retry_operation(
            "Load the incoming document",
            self.text_similarity_input_db_store.retrieve_document_from_tsp_id,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        if document is None:
            self.logger.error(
                "No text_similarity_input record exists for this document."
            )
            self.logger.info(
                f"Sentence processing stopped after "
                f"{perf_counter() - started_at:.2f} seconds because the "
                "document was not found."
            )
            return None, [], None

        self._log_document_context(document)

        # Refugee referral documents contain template sections that should not
        # become searchable sentences. Filter those sections before splitting.
        document = self.rrf_filterer.filter_rrfs(document)
        # The old Doc2Vec flow treated line breaks and the legacy Ê separator as
        # spaces. Keep that cleanup so sentence text and boundaries stay stable.
        cleaned_content = re.sub(r"[Ê\n]", " ", document.content or "")
        self.logger.info("Sentence splitting started.")
        sentences = self._clean_sentences(split_document(cleaned_content))
        self.logger.info(
            f"Sentence splitting completed with {len(sentences)} sentence(s)."
        )
        # A document can contain only spaces or separators after splitting.
        # Send no_content so the existing state-machine flow can handle it.
        if not sentences:
            result = self._send_processing_result(
                tsp_id=tsp_id,
                source_count=0,
                indexed_count=0,
                success=False,
                reason="no_content",
            )
            self.logger.info(
                "Sentence processing finished without indexing because the "
                "document has no sentence content."
            )
            self.logger.info(
                f"Sentence processing took {perf_counter() - started_at:.2f} "
                "seconds to complete."
            )
            return document, [], result

        sentence_vector_records = self._save_document_sentences(document, sentences)
        return document, sentence_vector_records, None

    def _log_document_context(self, document: TextSimilarityInput) -> None:
        """Log the non-PII identifiers used to process one document."""

        self.logger.info(
            f"Processing application_id={document.application_id}, "
            f"analysis_group={document.analysis_group}."
        )

    def _load_saved_document_sentences(
        self,
        tsp_id: TspId,
    ) -> list[SentencesVector]:
        """Load saved sentence records in pages for resume processing.

        Input: tsp_id=1040 with 1,200 saved sentences
        Output: all 1,200 SentencesVector records loaded in 500-record pages.
        """

        sentence_vector_records: list[SentencesVector] = []
        after_global_id = 0
        page_number = 0
        while True:
            page_number += 1
            sentence_vector_page = retry_operation(
                f"Load saved sentence page {page_number}",
                self.sentence_vector_db_store.load_sentences_batch_by_tsp_id,
                tsp_id,
                after_global_id,
                constants.DOCUMENT_SENTENCE_PAGE_SIZE,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
            if not sentence_vector_page:
                break

            sentence_vector_records.extend(sentence_vector_page)
            after_global_id = sentence_vector_page[-1].global_id
            if len(sentence_vector_page) < constants.DOCUMENT_SENTENCE_PAGE_SIZE:
                break
        return sentence_vector_records

    def _prepare_saved_sentence_keys(
        self,
        sentence_vector_records: list[SentencesVector],
    ) -> tuple[int, int]:
        """Generate only the keys missing from previously saved sentence records.

        Input: 700 saved records where 50 have no sentence_key.
        Output: (50, 650), meaning 50 keys were generated and 650 were reused.

        Existing rows prove sentence splitting already completed. Filling their
        missing keys lets a retried message resume without inserting duplicate
        sentences_vector records.
        """

        generated_key_count = 0
        reused_key_count = 0
        for sentence_vector_batch in batches(
            sentence_vector_records,
            constants.DOCUMENT_SENTENCE_PAGE_SIZE,
        ):
            generated, reused = retry_operation(
                "Save missing sentence keys for an existing document",
                self.sentence_vector_db_store.save_missing_sentence_keys,
                sentence_vector_batch,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
            generated_key_count += generated
            reused_key_count += reused
        return generated_key_count, reused_key_count

    def _send_processing_result(
        self,
        tsp_id: TspId,
        source_count: int,
        indexed_count: int,
        success: bool,
        reason: str,
    ) -> LiveResult:
        """Send the SQS result and return the document processing counts.

        Input: source_count=65, indexed_count=65, success=True, reason=""
        Output: a successful LiveResult after sending the next SQS message.
        """

        retry_operation(
            "Send sentence processing result to next queue",
            self._send_sqs_message,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        return {
            "tsp_id": tsp_id,
            "source_count": source_count,
            "indexed_count": indexed_count,
            "success": success,
            "reason": reason,
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

    def _save_document_sentences(
        self,
        document: TextSimilarityInput,
        sentences: list[str],
    ) -> list[SentencesVector]:
        """Save every sentence in one transaction and return its global ID.

        Input: 2,300 sentence strings with a database batch size of 1,000.
        Output: 2,300 records flushed in three batches and saved by one commit.

        If any batch fails, PostgreSQL rolls back every sentence for the
        document. The retry therefore starts again without partial records.
        """

        numbered_sentences = list(enumerate(sentences, start=1))
        sentence_vector_records = retry_operation(
            "Save all document sentences",
            self.sentence_vector_db_store.save_document_sentences,
            document,
            numbered_sentences,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Saved {len(sentence_vector_records)} sentence(s) in one database "
            "transaction."
        )
        return sentence_vector_records

    def _save_sentences_in_opensearch(
        self,
        sentence_vector_records: list[SentencesVector],
    ) -> int:
        """Save all document sentences to OpenSearch using concurrent batches.

        Input: 65 SentencesVector records and worker_batch_size=30
        Output: 65 after all three worker batches save their catalog and
        occurrence documents.
        """

        worker_batches = list(
            batches(sentence_vector_records, constants.WORKER_BATCH_SIZE)
        )
        self.logger.info(
            f"Indexing {len(sentence_vector_records)} sentence(s) in "
            f"{len(worker_batches)} worker batch(es)."
        )
        with ThreadPoolExecutor(
            max_workers=constants.THREAD_WORKER_COUNT,
            thread_name_prefix="sentence",
        ) as executor:
            return sum(
                executor.map(
                    self._save_sentence_batch_in_opensearch,
                    worker_batches,
                )
            )

    def _save_sentence_batch_in_opensearch(
        self,
        sentence_vector_records: list[SentencesVector],
    ) -> int:
        """Save one document sentence batch to both OpenSearch indexes.

        Input: SentencesVector records for global IDs 1 through 30.
        Output: 30 after missing catalog vectors and all occurrences are saved.

        This path does not regenerate keys or read vectors from PostgreSQL.
        Bedrock creates a vector only when a sentence key is missing in the
        selected catalog.
        """

        label = (
            f"Sentence worker batch for global IDs "
            f"{sentence_vector_records[0].global_id} through "
            f"{sentence_vector_records[-1].global_id}"
        )
        return self.batch_processor.save_document_sentence_batch(
            sentence_vector_records,
            label,
        )

    def _send_sqs_message(
        self,
        tsp_id: TspId,
    ) -> None:
        """Send the existing Doc2Vec result contract to the state machine.

        Input: tsp_id=1040
        Output message: {"source": "P2", "tsp_id": 1040,
        "sent_by": "doc2vec"}
        """

        queue_url = self.config.aws_sqs_send_event_queue_endpoint
        if not queue_url:
            raise QueueSendError("AWS SQS send event queue endpoint is not configured")

        message: LiveNextQueueMessage = {
            "source": "P2",
            "tsp_id": tsp_id,
            "sent_by": "doc2vec",
        }
        sent = self.aws_sqs.send_sqs_message(
            send_queue_url=queue_url,
            message_body=message,
            is_fifo=queue_url.endswith(".fifo"),
        )
        if not sent:
            raise QueueSendError("Failed to send the Doc2Vec state-machine message")

