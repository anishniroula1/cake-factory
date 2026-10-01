from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from sqlalchemy import bindparam, func, select, update
from sqlalchemy.orm import Session

from doc2vec_completed.opensearch.AwsSQS import AwsSQS
from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import (
    CatalogDocument,
    HistoricalResult,
    NextQueueMessage,
    OccurrenceDocument,
    TspId,
)
from doc2vec_completed.opensearch.custom_logger import CustomLogger
from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.models import Embeddings, SentencesVector
from doc2vec_completed.opensearch.opensearch import OpenSearchHelper
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.text import sentence_key


class HistoricalHandler:
    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch: OpenSearchHelper,
        aws_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        """Keep everything required for one HISTORICAL message."""

        self.config = config
        self.session_factory = session_factory
        self.opensearch = opensearch
        self.aws_sqs = aws_sqs
        self.logger = custom_logger

    def process(self, tsp_id: TspId) -> HistoricalResult:
        """Index already-split historical sentences and send the next message."""

        total = retry_operation(
            f"count HISTORICAL tsp_id {tsp_id}",
            self._count,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        if total == 0:
            raise PermanentDataError(
                f"sentences_vector has no rows for tsp_id {tsp_id}"
            )

        self.logger.info(f"HISTORICAL tsp_id={tsp_id} contains {total} sentence(s)")
        indexed = 0
        after_global_id = 0

        with ThreadPoolExecutor(
            max_workers=self.config.thread_worker_count,
            thread_name_prefix="historical-sentence",
        ) as executor:
            while True:
                rows = retry_operation(
                    f"load HISTORICAL tsp_id {tsp_id}",
                    self._load_page,
                    tsp_id,
                    after_global_id,
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )
                if not rows:
                    break

                # Historical rows already contain split sentences. Only create
                # the stable sentence keys that the two OpenSearch indexes use.
                retry_operation(
                    f"save HISTORICAL tsp_id {tsp_id} sentence keys",
                    self._save_keys,
                    rows,
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )

                # Split this database page into small jobs for the worker pool.
                # Run every job in parallel and add its row count to the total.
                worker_batches = list(batches(rows, self.config.worker_batch_size))
                indexed += sum(executor.map(self._run_worker, worker_batches))
                after_global_id = rows[-1].global_id
                self.logger.info(
                    f"HISTORICAL tsp_id={tsp_id} processed through "
                    f"global_id={after_global_id}"
                )

        # Publish only after every historical sentence is safely indexed. The
        # SQS processor deletes the source message after this method returns.
        retry_operation(
            f"publish HISTORICAL tsp_id {tsp_id}",
            self._publish,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )

        return {
            "reason": "HISTORICAL",
            "tsp_id": tsp_id,
            "source_count": total,
            "indexed_count": indexed,
        }

    def _count(self, tsp_id: TspId) -> int:
        """Count every historical sentence row for one document."""

        with self.session_factory() as session:
            statement = (
                select(func.count())
                .select_from(SentencesVector)
                .where(SentencesVector.tsp_id == tsp_id)
            )
            return int(session.scalar(statement) or 0)

    def _load_page(
        self,
        tsp_id: TspId,
        after_global_id: int,
    ) -> list[SentencesVector]:
        """Read the next historical page in stable global-ID order."""

        # Load enough rows to give every worker one full batch. This keeps all
        # workers busy without loading every sentence for the tsp_id at once.
        page_size = self.config.worker_batch_size * self.config.thread_worker_count
        with self.session_factory() as session:
            statement = (
                select(SentencesVector)
                .where(
                    SentencesVector.tsp_id == tsp_id,
                    SentencesVector.global_id > after_global_id,
                )
                .order_by(SentencesVector.global_id)
                .limit(page_size)
            )
            return list(session.scalars(statement))

    def _save_keys(self, rows: list[SentencesVector]) -> None:
        """Create sentence keys and update one historical database page."""

        current_time = datetime.now(UTC)
        updates = []
        for row in rows:
            row.sentence_key = sentence_key(row.sentence_content)
            row.updated_at = current_time
            updates.append(
                {
                    "record_global_id": row.global_id,
                    "record_sentence_key": row.sentence_key,
                    "record_updated_at": current_time,
                }
            )

        table = SentencesVector.__table__
        statement = (
            update(table)
            .where(table.c.global_id == bindparam("record_global_id"))
            .values(
                sentence_key=bindparam("record_sentence_key"),
                updated_at=bindparam("record_updated_at"),
            )
        )
        with self.session_factory() as session:
            session.execute(statement, updates)
            session.commit()

    def _run_worker(self, rows: list[SentencesVector]) -> int:
        """Retry one historical worker batch up to the configured limit."""

        label = f"HISTORICAL global_id {rows[0].global_id}-{rows[-1].global_id}"
        return retry_operation(
            label,
            self._process_worker,
            rows,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )

    def _process_worker(self, rows: list[SentencesVector]) -> int:
        """Write missing catalog entries and every historical occurrence."""

        embeddings = self._load_embeddings([row.global_id for row in rows])
        existing_keys = self.opensearch.existing_ids(
            self.config.catalog_alias,
            [row.sentence_key for row in rows],
        )

        with_vectors: list[CatalogDocument] = []
        need_vectors: list[CatalogDocument] = []
        seen_keys: set[str] = set()
        for row in rows:
            if row.sentence_key in existing_keys or row.sentence_key in seen_keys:
                continue
            seen_keys.add(row.sentence_key)
            document: CatalogDocument = {
                "_id": row.sentence_key,
                "sentenceKey": row.sentence_key,
                "sentenceContent": row.sentence_content,
                "createdAt": row.created_at.isoformat(),
                "updatedAt": row.updated_at.isoformat(),
            }
            vector = embeddings.get(row.global_id)
            if vector is None:
                need_vectors.append(document)
                continue
            self.opensearch.validate_vector(vector)
            document["sentenceContentVector"] = vector
            with_vectors.append(document)

        # Documents with a saved Titan vector bypass the embedding pipeline.
        # Only documents without a saved vector call the Bedrock pipeline.
        self.opensearch.bulk_index(
            self.config.catalog_alias,
            with_vectors,
            needs_embedding=False,
        )
        self.opensearch.bulk_index(
            self.config.catalog_alias,
            need_vectors,
            needs_embedding=True,
        )

        occurrences: list[OccurrenceDocument] = []
        for row in rows:
            occurrences.append(
                {
                    "_id": row.global_id,
                    "globalId": row.global_id,
                    "applicationId": row.application_id,
                    "tspId": str(row.tsp_id),
                    "sectionName": row.section_name,
                    "sentIdLocal": row.sent_id_local,
                    "sentenceContent": row.sentence_content,
                    "sentenceKey": row.sentence_key,
                    "sourceType": row.source_type,
                    "analysisGroup": row.analysis_group,
                    "isNonMatcher": row.is_non_matcher,
                    "isTracer": row.is_tracer,
                    "isFormLanguage": row.is_form_language,
                    "isCurrent": row.is_current,
                    "createdAt": row.created_at.isoformat(),
                    "updatedAt": row.updated_at.isoformat(),
                }
            )
        self.opensearch.bulk_index(
            self.config.occurrence_alias,
            occurrences,
            needs_embedding=False,
        )
        return len(rows)

    def _load_embeddings(self, global_ids: list[int]) -> dict[int, list[float]]:
        """Read saved Titan type-2 vectors for one worker batch."""

        with self.session_factory() as session:
            statement = select(Embeddings).where(
                Embeddings.global_id.in_(global_ids),
                Embeddings.embedding_type == self.config.titan_embedding_type,
            )
            return {
                row.global_id: list(row.embeddings)
                for row in session.scalars(statement)
                if row.embeddings
            }

    def _publish(self, tsp_id: TspId) -> None:
        """Send the completed historical document to Sentence Similarity."""

        queue_url = self.config.sentence_similarity_backfill_queue_url
        if not queue_url:
            return

        message: NextQueueMessage = {
            "tsp_id": tsp_id,
            "reason": "HISTORICAL",
        }
        sent = self.aws_sqs.send_sqs_message(
            send_queue_url=queue_url,
            message_body=message,
            is_fifo=queue_url.endswith(".fifo"),
        )
        if not sent:
            raise RuntimeError(f"failed to send tsp_id {tsp_id} to Sentence Similarity")
