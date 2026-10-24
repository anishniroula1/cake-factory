from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from sqlalchemy.orm import Session

from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import (
    LiveNextQueueMessage,
    LiveResult,
    TspId,
)
from doc2vec_completed.opensearch.db_store import (
    EmbeddingDBStore,
    SentenceVectorDBStore,
    TextSimilarityInputDBStore,
)
from doc2vec_completed.opensearch.errors import QueueSendError
from doc2vec_completed.opensearch.open_search_helper import OpenSearchHelper
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.sentence_batch_processor import (
    SentenceBatchProcessor,
)
from doc2vec_completed.opensearch.sentence_splitter import split_document
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
        sentence_splitter: Callable[[str], list[str]] = split_document,
    ) -> None:
        self.config = config
        self.opensearch_helper = opensearch_helper
        self.aws_sqs = aws_sqs
        self.logger = custom_logger
        self.sentence_splitter = sentence_splitter
        self.document_db_store = TextSimilarityInputDBStore(session_factory)
        self.sentence_vector_db_store = SentenceVectorDBStore(session_factory)
        self.embedding_db_store = EmbeddingDBStore(session_factory)
        self.batch_processor = SentenceBatchProcessor(
            config,
            self.sentence_vector_db_store,
            self.embedding_db_store,
            opensearch_helper,
            custom_logger,
        )

    def process(self, tsp_id: TspId) -> LiveResult | None:
        """Create sentence rows and OpenSearch records for one live document.

        Input: tsp_id=1040
        Output: {"tsp_id": 1040, "source_count": 65,
        "indexed_count": 65, "success": True, "reason": ""}

        A document with no usable text sends success=False and reason=no_content
        so the existing state-machine flow can handle it without retrying SQS.
        """

        started_at = perf_counter()
        self.logger.info("Live sentence processing started.")
        document = retry_operation(
            "Load the incoming document",
            self.document_db_store.load_document,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        if document is None:
            self.logger.error("No text_similarity_input row exists for this document.")
            return None

        sentences = self.sentence_splitter(document.content or "")
        if not sentences:
            retry_operation(
                "Send the no-content document message",
                self._publish,
                tsp_id,
                False,
                "no_content",
                wait_seconds=self.config.retry_seconds,
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
        catalog_alias = self.batch_processor.catalog_alias(document.analysis_group)
        indexed = self._index_sentences(rows)

        retry_operation(
            "Refresh indexed live sentences",
            self.opensearch_helper.refresh_aliases,
            [catalog_alias, self.config.occurrence_alias],
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        retry_operation(
            "Send the completed live document message",
            self._publish,
            tsp_id,
            True,
            "",
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Live sentence processing completed. Source count: {len(sentences)}. "
            f"Indexed count: {indexed}."
        )
        self.logger.info(
            f"Live sentence processing took {perf_counter() - started_at:.2f} "
            "seconds to complete."
        )
        return {
            "tsp_id": tsp_id,
            "source_count": len(sentences),
            "indexed_count": indexed,
            "success": True,
            "reason": "",
        }

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
            batches(numbered_sentences, self.config.live_database_batch_size),
            start=1,
        ):
            rows = retry_operation(
                f"Save live sentence batch {batch_number}",
                self.sentence_vector_db_store.save_live_sentence_batch,
                document,
                sentence_batch,
                wait_seconds=self.config.retry_seconds,
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

        worker_batches = list(batches(rows, self.config.worker_batch_size))
        self.logger.info(
            f"Indexing {len(rows)} live sentence(s) in "
            f"{len(worker_batches)} worker batch(es)."
        )
        with ThreadPoolExecutor(
            max_workers=self.config.thread_worker_count,
            thread_name_prefix="live-sentence",
        ) as executor:
            return sum(executor.map(self._run_worker, worker_batches))

    def _run_worker(self, rows: list[SentencesVector]) -> int:
        """Write one live sentence batch to catalog and occurrence indexes."""

        label = (
            f"Live worker batch for global IDs {rows[0].global_id} "
            f"through {rows[-1].global_id}"
        )
        return self.batch_processor.process(rows, label)

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
