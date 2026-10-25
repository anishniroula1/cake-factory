from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from sqlalchemy.orm import Session

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import (
    TrainingDocument,
    TrainModelResult,
    TspId,
)
from doc2vec_completed.opensearch.db_store import (
    EmbeddingDBStore,
    SentenceVectorDBStore,
)
from doc2vec_completed.opensearch.open_search_helper import OpenSearchHelper
from doc2vec_completed.opensearch.opensearch_store import (
    TrainingOpenSearchStore,
)
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.text import sentence_key
from ta_db_connection.atables import SentencesVector
from ta_utils.logger.CustomLogger import CustomLogger


class TrainModelHandler:
    """Load representative sentences into the OpenSearch IVF training index."""

    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch_helper: OpenSearchHelper,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.training_open_search_store = TrainingOpenSearchStore(
            config,
            opensearch_helper,
        )
        self.logger = custom_logger
        self.sentence_vector_db_store = SentenceVectorDBStore(session_factory)
        self.embedding_db_store = EmbeddingDBStore(session_factory)

    def process(self, tsp_id: TspId) -> TrainModelResult | None:
        """Index every unique sentence from one document for IVF model training.

        Input: tsp_id=1040
        Output: source, unique, and newly indexed document counts. Sentence keys
        are created in memory and are not saved to sentences_vector.
        """

        started_at = perf_counter()
        total = retry_operation(
            "Count model-training sentences",
            self.sentence_vector_db_store.count_training_sentences_by_tsp_id,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        if total == 0:
            self.logger.error(
                "No sentences_vector records are available for model training."
            )
            return None

        self.logger.info(f"Found {total} model-training sentence(s) to process.")
        after_global_id = 0
        source_count = 0
        indexed_count = 0
        seen_keys: set[str] = set()
        page_number = 0

        with ThreadPoolExecutor(
            max_workers=constants.TRAIN_MODEL_WORKER_COUNT,
            thread_name_prefix="training",
        ) as executor:
            while True:
                page_number += 1
                sentence_vector_records = retry_operation(
                    f"Load model-training page {page_number}",
                    self.sentence_vector_db_store.load_training_sentence_page,
                    tsp_id,
                    after_global_id,
                    constants.OPENSEARCH_BATCH_SIZE,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
                if not sentence_vector_records:
                    break

                # Training must contain normal matching sentences only. Keep a
                # second check in case an unexpected record passes the query.
                eligible_sentence_vector_records = [
                    sentence_vector_record
                    for sentence_vector_record in sentence_vector_records
                    if self._is_training_eligible(sentence_vector_record)
                ]
                embeddings = retry_operation(
                    f"Load saved vectors for model-training page {page_number}",
                    self.embedding_db_store.load_titan_embeddings_by_global_ids,
                    [
                        sentence_vector_record.global_id
                        for sentence_vector_record in eligible_sentence_vector_records
                    ],
                    constants.TITAN_EMBEDDING_TYPE,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
                documents = self._build_unique_training_documents(
                    eligible_sentence_vector_records,
                    embeddings,
                    seen_keys,
                )
                worker_batches = list(batches(documents, constants.WORKER_BATCH_SIZE))
                self.logger.info(
                    f"Processing model-training page {page_number} with "
                    f"{len(eligible_sentence_vector_records)} eligible sentence(s), "
                    f"{len(sentence_vector_records) - len(eligible_sentence_vector_records)} "
                    "excluded sentence(s), and "
                    f"{len(documents)} new unique key(s)."
                )
                indexed_count += sum(
                    executor.map(
                        self._save_training_batch_in_opensearch,
                        worker_batches,
                    )
                )
                source_count += len(eligible_sentence_vector_records)
                after_global_id = sentence_vector_records[-1].global_id

        if indexed_count:
            retry_operation(
                "Refresh the model-training index",
                self.training_open_search_store.refresh,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
        self.logger.info(
            f"Model-training processing completed. Source count: {source_count}. "
            f"Unique count: {len(seen_keys)}. Indexed count: {indexed_count}."
        )
        self.logger.info(
            f"Model-training processing took {perf_counter() - started_at:.2f} seconds to complete."
        )
        return {
            "reason": "TRAIN_MODEL",
            "tsp_id": tsp_id,
            "source_count": source_count,
            "unique_count": len(seen_keys),
            "indexed_count": indexed_count,
        }

    @staticmethod
    def _is_training_eligible(
        sentence_vector_record: SentencesVector,
    ) -> bool:
        """Return True only for a normal sentence that can train the IVF model.

        Input: a sentence with is_non_matcher=False, is_tracer=False, and
        is_form_language=False.
        Output: True. Any one of those fields being True returns False.
        """

        return not (
            sentence_vector_record.is_non_matcher
            or sentence_vector_record.is_tracer
            or sentence_vector_record.is_form_language
        )

    def _build_unique_training_documents(
        self,
        sentence_vector_records: list[SentencesVector],
        embeddings: dict[int, list[float]],
        seen_keys: set[str],
    ) -> list[TrainingDocument]:
        """Build one training document for each sentence key not already seen.

        Input: two SentencesVector records share key A; one has a saved vector.
        Output: one key-A document containing the saved vector.

        Keys found on an earlier page are skipped. When duplicate records exist
        on the current page, the record with a saved Titan vector is preferred.
        """

        documents_by_key: dict[str, TrainingDocument] = {}
        for sentence_vector_record in sentence_vector_records:
            key = sentence_key(sentence_vector_record.sentence_content)
            if key in seen_keys:
                continue
            timestamp = sentence_vector_record.last_modified.isoformat()
            document: TrainingDocument = {
                "_id": key,
                "sentenceKey": key,
                "sentenceContent": sentence_vector_record.sentence_content,
                "createdAt": timestamp,
                "updatedAt": timestamp,
            }
            vector = embeddings.get(sentence_vector_record.global_id)
            if vector is not None:
                self.training_open_search_store.validate_vector(vector)
                document["sentenceContentVector"] = vector

            current = documents_by_key.get(key)
            if current is None or (
                "sentenceContentVector" not in current
                and "sentenceContentVector" in document
            ):
                documents_by_key[key] = document

        seen_keys.update(documents_by_key)
        return list(documents_by_key.values())

    def _save_training_batch_in_opensearch(
        self,
        documents: list[TrainingDocument],
    ) -> int:
        """Save one unique training-document batch in OpenSearch.

        Input: up to 30 unique training documents.
        Output: the number of keys newly saved after existing keys are removed.

        Documents with vectors are written directly. Documents without vectors
        use the Bedrock embedding pipeline.
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
            self.training_open_search_store.find_existing_sentence_keys,
            keys,
            wait_seconds=constants.RETRY_SECONDS,
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
            self.training_open_search_store.index_saved_vectors,
            saved_vectors,
            wait_seconds=constants.RETRY_SECONDS,
            max_attempts=constants.OPENSEARCH_BULK_RETRY_COUNT + 1,
            custom_logger=self.logger,
        )
        retry_operation(
            f"Create Bedrock vectors for {batch_label}",
            self.training_open_search_store.index_with_bedrock,
            bedrock_vectors,
            wait_seconds=constants.RETRY_SECONDS,
            max_attempts=constants.OPENSEARCH_BULK_RETRY_COUNT + 1,
            custom_logger=self.logger,
        )
        return len(new_documents)
