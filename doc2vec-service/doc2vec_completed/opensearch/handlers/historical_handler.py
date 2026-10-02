from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from sqlalchemy import bindparam, func, select, update
from sqlalchemy.orm import Session, joinedload

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

        self.logger.info("Historical sentence processing started.")
        self.logger.info("Counting historical sentences.")
        total = retry_operation(
            "Count historical sentences",
            self._count,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        if total == 0:
            raise PermanentDataError(
                "sentences_vector has no rows for the requested historical document"
            )

        self.logger.info(
            f"Found {total} historical sentence(s) to process."
        )
        indexed = 0
        after_global_id = 0
        page_number = 0

        with ThreadPoolExecutor(
            max_workers=self.config.thread_worker_count,
            thread_name_prefix="historical-sentence",
        ) as executor:
            while True:
                page_number += 1
                self.logger.info(
                    f"Loading historical page {page_number} after global ID "
                    f"{after_global_id}."
                )
                rows = retry_operation(
                    f"Load historical page {page_number}",
                    self._load_page,
                    tsp_id,
                    after_global_id,
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )
                if not rows:
                    self.logger.info(
                        f"Historical page {page_number} is empty. All pages "
                        "have been loaded."
                    )
                    break

                self.logger.info(
                    f"Historical page {page_number} loaded {len(rows)} "
                    f"sentence(s), global IDs {rows[0].global_id} through "
                    f"{rows[-1].global_id}."
                )

                # Historical rows already contain split sentences. Only create
                # the stable sentence keys that the two OpenSearch indexes use.
                self.logger.info(
                    f"Creating and saving sentence keys for historical page "
                    f"{page_number}."
                )
                retry_operation(
                    f"Save sentence keys for historical page {page_number}",
                    self._save_keys,
                    rows,
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )
                self.logger.info(
                    f"Saved {len(rows)} sentence key(s) for historical page "
                    f"{page_number}."
                )

                # Split this database page into small jobs for the worker pool.
                # Run every job in parallel and add its row count to the total.
                worker_batches = list(batches(rows, self.config.worker_batch_size))
                self.logger.info(
                    f"Starting {len(worker_batches)} worker batch(es) for "
                    f"historical page {page_number}."
                )
                page_indexed = sum(executor.map(self._run_worker, worker_batches))
                indexed += page_indexed
                after_global_id = rows[-1].global_id
                self.logger.info(
                    f"Historical page {page_number} completed. Indexed "
                    f"{page_indexed} sentence(s) from this page and {indexed} "
                    f"sentence(s) in total through global ID {after_global_id}."
                )

        # Publish only after every historical sentence is safely indexed. The
        # SQS processor deletes the source message after this method returns.
        self.logger.info("Sending the completed historical message to the next queue.")
        retry_operation(
            "Send the completed historical message",
            self._publish,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        self.logger.info("Sent the completed historical message to the next queue.")
        self.logger.info(
            f"Historical sentence processing completed. Source count: {total}. "
            f"Indexed count: {indexed}."
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
                .options(joinedload(SentencesVector.tsp))
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
            row.last_modified = current_time
            updates.append(
                {
                    "record_global_id": row.global_id,
                    "record_sentence_key": row.sentence_key,
                    "record_last_modified": current_time,
                }
            )

        table = SentencesVector.__table__
        statement = (
            update(table)
            .where(table.c.global_id == bindparam("record_global_id"))
            .values(
                sentence_key=bindparam("record_sentence_key"),
                last_modified=bindparam("record_last_modified"),
            )
        )
        with self.session_factory() as session:
            session.execute(statement, updates)
            session.commit()

    def _run_worker(self, rows: list[SentencesVector]) -> int:
        """Retry one historical worker batch up to the configured limit."""

        label = self._batch_label(rows)
        self.logger.info(
            f"{label} started with {len(rows)} sentence(s)."
        )
        indexed_count = retry_operation(
            label,
            self._process_worker,
            rows,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"{label} completed with {indexed_count} indexed sentence(s)."
        )
        return indexed_count

    def _process_worker(self, rows: list[SentencesVector]) -> int:
        """Write one document's catalog entries and sentence occurrences."""

        label = self._batch_label(rows)
        stage = "catalog selection"
        try:
            document = rows[0].tsp
            catalog_alias = self._catalog_alias(
                document.analysis_group,
            )
            self.logger.info(
                f"{label} selected catalog {catalog_alias} for analysis group "
                f"{document.analysis_group}."
            )

            stage = "saved embedding lookup"
            embeddings = self._load_embeddings([row.global_id for row in rows])
            self.logger.info(
                f"{label} loaded {len(embeddings)} saved Titan embedding(s)."
            )

            stage = "catalog indexing"
            self._index_catalog_rows(catalog_alias, rows, embeddings, label)

            stage = "occurrence indexing"
            occurrences = self._occurrence_documents(rows)
            self.logger.info(
                f"{label} is writing {len(occurrences)} occurrence document(s) "
                f"to {self.config.occurrence_alias}."
            )
            self.opensearch.bulk_index(
                self.config.occurrence_alias,
                occurrences,
                needs_embedding=False,
            )
            self.logger.info(
                f"{label} wrote {len(occurrences)} occurrence document(s)."
            )
            return len(rows)
        except Exception as error:
            self.logger.error(
                f"{label} failed during {stage}: {str(error)[:500]}"
            )
            raise

    def _batch_label(self, rows: list[SentencesVector]) -> str:
        """Return the stable log label for one worker batch."""

        return (
            f"Historical worker batch for global IDs {rows[0].global_id} "
            f"through {rows[-1].global_id}"
        )

    def _occurrence_documents(
        self,
        rows: list[SentencesVector],
    ) -> list[OccurrenceDocument]:
        """Build occurrence documents from sentence and joined document rows."""

        document = rows[0].tsp
        occurrences: list[OccurrenceDocument] = []
        for row in rows:
            occurrences.append(
                {
                    "_id": row.global_id,
                    "globalId": row.global_id,
                    "applicationId": document.application_id,
                    "tspId": document.tsp_id,
                    "sectionName": document.section_name,
                    "sentIdLocal": row.sent_id_local,
                    "sentenceContent": row.sentence_content,
                    "sentenceKey": row.sentence_key,
                    "sourceType": row.source_type,
                    "analysisGroup": document.analysis_group,
                    "isCurrent": document.is_current,
                    "createdAt": row.last_modified.isoformat(),
                    "updatedAt": row.last_modified.isoformat(),
                }
            )
        return occurrences

    def _catalog_alias(self, analysis_group: str) -> str:
        """Return the one catalog selected for a document's analysis group."""

        normalized_group = analysis_group.strip().casefold()
        if normalized_group == "asylee":
            return self.config.asylee_catalog_alias
        if normalized_group == "refugee":
            return self.config.refugee_catalog_alias
        raise PermanentDataError(
            f"unsupported analysis_group {analysis_group!r}"
        )

    def _index_catalog_rows(
        self,
        catalog_alias: str,
        rows: list[SentencesVector],
        embeddings: dict[int, list[float]],
        batch_label: str,
    ) -> None:
        """Write missing unique keys from one analysis group to its catalog."""

        existing_keys = self.opensearch.existing_ids(
            catalog_alias,
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
                "createdAt": row.last_modified.isoformat(),
                "updatedAt": row.last_modified.isoformat(),
            }
            vector = embeddings.get(row.global_id)
            if vector is None:
                need_vectors.append(document)
                continue
            self.opensearch.validate_vector(vector)
            document["sentenceContentVector"] = vector
            with_vectors.append(document)

        # Saved Titan vectors bypass the pipeline. Only missing vectors call
        # the selected catalog's Bedrock embedding pipeline.
        self.logger.info(
            f"{batch_label} is writing catalog {catalog_alias}. Existing keys: "
            f"{len(existing_keys)}. Saved vectors: {len(with_vectors)}. "
            f"Vectors for Bedrock: {len(need_vectors)}."
        )
        self.opensearch.bulk_index(
            catalog_alias,
            with_vectors,
            needs_embedding=False,
        )
        self.opensearch.bulk_index(
            catalog_alias,
            need_vectors,
            needs_embedding=True,
        )
        self.logger.info(
            f"{batch_label} completed catalog {catalog_alias} with "
            f"{len(with_vectors) + len(need_vectors)} indexed document(s)."
        )

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
            raise RuntimeError("failed to send the historical message to Sentence Similarity")
