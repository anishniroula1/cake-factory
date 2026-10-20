from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from sqlalchemy import func, select
from sqlalchemy.orm import Session, load_only

from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import (
    TrainingDocument,
    TrainModelResult,
    TspId,
)
from doc2vec_completed.opensearch.opensearch import OpenSearchHelper
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.text import sentence_key
from ta_db_connection.atables import Embeddings, SentencesVector
from ta_utils.logger.CustomLogger import CustomLogger


class TrainModelHandler:
    """Load representative sentences into the OpenSearch IVF training index."""

    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch: OpenSearchHelper,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.session_factory = session_factory
        self.opensearch = opensearch
        self.logger = custom_logger

    def process(self, tsp_id: TspId) -> TrainModelResult | None:
        """Index every unique sentence from one document for IVF model training.

        Input: tsp_id=1040
        Output: source, unique, and newly indexed document counts. Sentence keys
        are created in memory and are not saved to sentences_vector.
        """

        started_at = perf_counter()
        total = retry_operation(
            "Count model-training sentences",
            self._count_sentences,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        if total == 0:
            self.logger.error(
                "No sentences_vector rows are available for model training."
            )
            return None

        self.logger.info(f"Found {total} model-training sentence(s) to process.")
        after_global_id = 0
        source_count = 0
        indexed_count = 0
        seen_keys: set[str] = set()
        page_number = 0

        with ThreadPoolExecutor(
            max_workers=self.config.train_model_worker_count,
            thread_name_prefix="training",
        ) as executor:
            while True:
                page_number += 1
                rows = retry_operation(
                    f"Load model-training page {page_number}",
                    self._load_sentence_page,
                    tsp_id,
                    after_global_id,
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )
                if not rows:
                    break

                # Training must contain normal matching sentences only. Keep a
                # second check here in case an unexpected row passes the query.
                eligible_rows = [row for row in rows if self._is_training_eligible(row)]
                embeddings = retry_operation(
                    f"Load saved vectors for model-training page {page_number}",
                    self._load_embeddings,
                    [row.global_id for row in eligible_rows],
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )
                documents = self._prepare_unique_documents(
                    eligible_rows,
                    embeddings,
                    seen_keys,
                )
                worker_batches = list(batches(documents, self.config.worker_batch_size))
                self.logger.info(
                    f"Processing model-training page {page_number} with "
                    f"{len(eligible_rows)} eligible sentence(s), "
                    f"{len(rows) - len(eligible_rows)} excluded sentence(s), and "
                    f"{len(documents)} new unique key(s)."
                )
                indexed_count += sum(
                    executor.map(self._index_training_batch, worker_batches)
                )
                source_count += len(eligible_rows)
                after_global_id = rows[-1].global_id

        if indexed_count:
            retry_operation(
                "Refresh the model-training index",
                self.opensearch.refresh_aliases,
                [self.config.training_alias],
                wait_seconds=self.config.retry_seconds,
                custom_logger=self.logger,
            )
        self.logger.info(
            f"Model-training processing completed. Source count: {source_count}. "
            f"Unique count: {len(seen_keys)}. Indexed count: {indexed_count}."
        )
        self.logger.info(f"Model-training processing took {perf_counter() - started_at:.2f} seconds to complete.")
        return {
            "reason": "TRAIN_MODEL",
            "tsp_id": tsp_id,
            "source_count": source_count,
            "unique_count": len(seen_keys),
            "indexed_count": indexed_count,
        }

    def _count_sentences(self, tsp_id: TspId) -> int:
        """Count sentence rows for one training document.

        Input: tsp_id=1040
        Output: 1000 when the document contains 1000 sentence rows.
        """

        with self.session_factory() as session:
            statement = (
                select(func.count())
                .select_from(SentencesVector)
                .where(
                    SentencesVector.tsp_id == tsp_id,
                    SentencesVector.is_non_matcher.is_(False),
                    SentencesVector.is_tracer.is_(False),
                    SentencesVector.is_form_language.is_(False),
                )
            )
            return int(session.scalar(statement) or 0)

    def _load_sentence_page(
        self,
        tsp_id: TspId,
        after_global_id: int,
    ) -> list[SentencesVector]:
        """Load the next ordered page used to build training documents.

        Input: tsp_id=1040 and after_global_id=0
        Output: up to 1000 sentence rows ordered by global_id.
        """

        with self.session_factory() as session:
            statement = (
                select(SentencesVector)
                .options(
                    load_only(
                        SentencesVector.global_id,
                        SentencesVector.sentence_content,
                        SentencesVector.last_modified,
                        SentencesVector.is_non_matcher,
                        SentencesVector.is_tracer,
                        SentencesVector.is_form_language,
                    )
                )
                .where(
                    SentencesVector.tsp_id == tsp_id,
                    SentencesVector.global_id > after_global_id,
                    SentencesVector.is_non_matcher.is_(False),
                    SentencesVector.is_tracer.is_(False),
                    SentencesVector.is_form_language.is_(False),
                )
                .order_by(SentencesVector.global_id)
                .limit(self.config.opensearch_batch_size)
            )
            return list(session.scalars(statement))

    @staticmethod
    def _is_training_eligible(row: SentencesVector) -> bool:
        """Return True only for a normal sentence that can train the IVF model.

        Input: a sentence with is_non_matcher=False, is_tracer=False, and
        is_form_language=False.
        Output: True. Any one of those fields being True returns False.
        """

        return not (row.is_non_matcher or row.is_tracer or row.is_form_language)

    def _load_embeddings(self, global_ids: list[int]) -> dict[int, list[float]]:
        """Load existing Titan vectors for one page of sentence global IDs.

        Input: global_ids=[10, 20]
        Output: {10: [512 float values]} when only global ID 10 has embedding type 2.
        """

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

    def _prepare_unique_documents(
        self,
        rows: list[SentencesVector],
        embeddings: dict[int, list[float]],
        seen_keys: set[str],
    ) -> list[TrainingDocument]:
        """Create one document per new key and prefer a duplicate row's saved vector.

        Input: sentence rows, vectors keyed by global_id, and keys seen on earlier pages.
        Output: unique training documents that have not appeared in this message.
        """

        documents_by_key: dict[str, TrainingDocument] = {}
        for row in rows:
            key = sentence_key(row.sentence_content)
            if key in seen_keys:
                continue
            timestamp = row.last_modified.isoformat()
            document: TrainingDocument = {
                "_id": key,
                "sentenceKey": key,
                "sentenceContent": row.sentence_content,
                "createdAt": timestamp,
                "updatedAt": timestamp,
            }
            vector = embeddings.get(row.global_id)
            if vector is not None:
                self.opensearch.validate_vector(vector)
                document["sentenceContentVector"] = vector

            current = documents_by_key.get(key)
            if current is None or (
                "sentenceContentVector" not in current
                and "sentenceContentVector" in document
            ):
                documents_by_key[key] = document

        seen_keys.update(documents_by_key)
        return list(documents_by_key.values())

    def _index_training_batch(self, documents: list[TrainingDocument]) -> int:
        """Index new keys, reuse saved vectors, and generate only missing vectors.

        Input: up to 30 unique training documents.
        Output: the number of documents that did not already exist in OpenSearch.
        """

        if not documents:
            return 0
        batch_label = (
            f"training keys {documents[0]['sentenceKey'][:12]} through "
            f"{documents[-1]['sentenceKey'][:12]}"
        )
        keys = [document["sentenceKey"] for document in documents]
        existing_keys = retry_operation(
            f"Check existing {batch_label}",
            self.opensearch.existing_ids,
            self.config.training_alias,
            keys,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        new_documents = [
            document
            for document in documents
            if document["sentenceKey"] not in existing_keys
        ]
        saved_vectors = [
            document
            for document in new_documents
            if "sentenceContentVector" in document
        ]
        bedrock_vectors = [
            document
            for document in new_documents
            if "sentenceContentVector" not in document
        ]
        retry_operation(
            f"Write saved vectors for {batch_label}",
            self.opensearch.bulk_index,
            self.config.training_alias,
            saved_vectors,
            False,
            wait_seconds=self.config.retry_seconds,
            max_attempts=self.config.opensearch_bulk_retry_count + 1,
            custom_logger=self.logger,
        )
        retry_operation(
            f"Create Bedrock vectors for {batch_label}",
            self.opensearch.bulk_index,
            self.config.training_alias,
            bedrock_vectors,
            True,
            wait_seconds=self.config.retry_seconds,
            max_attempts=self.config.opensearch_bulk_retry_count + 1,
            custom_logger=self.logger,
        )
        return len(new_documents)
