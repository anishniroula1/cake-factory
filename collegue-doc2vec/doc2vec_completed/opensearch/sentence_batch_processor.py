from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.contracts import CatalogDocument, Classification
from doc2vec_completed.opensearch.db_store import (
    EmbeddingDBStore,
    SentenceVectorDBStore,
)
from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.opensearch_helper import OpenSearchHelper
from doc2vec_completed.opensearch.opensearch_store import (
    SentenceCatalogOpenSearchStore,
    SentenceOccurrenceOpenSearchStore,
    TracerOpenSearchStore,
)
from doc2vec_completed.opensearch.retry import retry_operation, safe_error_summary
from doc2vec_completed.opensearch.text import sentence_word_count
from doc2vec_completed.opensearch.tracer_filtering_service import (
    TracerFilteringService,
)
from ta_db_connection.atatables import SentencesVector
from ta_utils.logger.CustomLogger import CustomLogger


class SentenceBatchProcessor:
    """Write incoming or historical sentence batches to both OpenSearch indexes."""

    def __init__(
        self,
        opensearch_helper: OpenSearchHelper,
        custom_logger: CustomLogger,
    ) -> None:
        self.opensearch_helper = opensearch_helper
        self.sentence_catalog_opensearch_store = SentenceCatalogOpenSearchStore(
            opensearch_helper,
        )
        self.sentence_occurrence_opensearch_store = SentenceOccurrenceOpenSearchStore(
            opensearch_helper
        )
        self.tracer_opensearch_store = TracerOpenSearchStore(opensearch_helper)
        self.tracer_filtering_service = TracerFilteringService(
            self.sentence_catalog_opensearch_store,
            self.tracer_opensearch_store,
            custom_logger,
        )
        self.logger = custom_logger

    def process_historical_sentence_batch(
        self,
        sentence_vector_records: list[SentencesVector],
        label: str,
        sentence_vector_db_store: SentenceVectorDBStore,
        embedding_db_store: EmbeddingDBStore,
    ) -> int:
        """Prepare and save one historical batch in catalog and occurrence indexes.

        Input: 30 SentencesVector records for global IDs 3001 through 3030.
        Output: 30 after all 30 occurrence documents and missing catalog
        documents are saved.

        The method prepares keys and word counts in memory, reuses saved Titan
        vectors, classifies the keys, and writes every database field once.
        """

        generated_count, reused_count = (
            sentence_vector_db_store.prepare_sentence_metadata(
                sentence_vector_records
            )
        )

        document = sentence_vector_records[0].tsp
        catalog_alias = self.select_catalog_alias(document.analysis_group)
        embeddings = retry_operation(
            f"Load saved embeddings for {label}",
            embedding_db_store.load_titan_embeddings_by_global_ids,
            [
                sentence_vector_record.global_id
                for sentence_vector_record in sentence_vector_records
            ],
            constants.TITAN_EMBEDDING_TYPE,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        existing_count, saved_vector_count, bedrock_count = self._ensure_catalog_documents(
            catalog_alias,
            sentence_vector_records,
            embeddings,
            label,
        )
        classifications = self._classify_sentence_batch(
            catalog_alias,
            sentence_vector_records,
            label,
        )
        retry_operation(
            f"Save sentence metadata and classifications for {label}",
            sentence_vector_db_store.save_sentence_metadata_and_classifications,
            sentence_vector_records,
            classifications,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )

        occurrence_count = self._index_occurrence_documents(
            sentence_vector_records,
            label,
        )
        created_catalog_count = saved_vector_count + bedrock_count
        duplicate_sentence_count = len(sentence_vector_records) - len(classifications)
        self.logger.info(
            f"{label} completed: {generated_count} new key(s), "
            f"{reused_count} reused key(s); catalog keys: "
            f"{created_catalog_count} created ({saved_vector_count} with saved "
            f"vectors and {bedrock_count} with Bedrock), {existing_count} "
            f"reused; {duplicate_sentence_count} duplicate occurrence(s) reused "
            "an already handled sentence key; "
            f"{occurrence_count} occurrence(s) indexed."
        )
        return len(sentence_vector_records)

    def process_saved_document_sentence_batch(
        self,
        sentence_vector_records: list[SentencesVector],
        label: str,
        sentence_vector_db_store: SentenceVectorDBStore,
    ) -> int:
        """Save one incoming document batch in catalog and occurrence indexes.

        Input: 30 SentencesVector records for global IDs 1 through 30.
        Output: 30 after catalog and occurrence writes complete.

        This resume path prepares metadata, classifies every key, and updates
        each existing database row once before saving occurrences.
        """

        generated_count, reused_count = (
            sentence_vector_db_store.prepare_sentence_metadata(
                sentence_vector_records
            )
        )
        document = sentence_vector_records[0].tsp
        catalog_alias = self.select_catalog_alias(document.analysis_group)
        existing_count, _, bedrock_count = self._ensure_catalog_documents(
            catalog_alias,
            sentence_vector_records,
            {},
            label,
        )
        classifications = self._classify_sentence_batch(
            catalog_alias,
            sentence_vector_records,
            label,
        )
        retry_operation(
            f"Save sentence metadata and classifications for {label}",
            sentence_vector_db_store.save_sentence_metadata_and_classifications,
            sentence_vector_records,
            classifications,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        occurrence_count = self._index_occurrence_documents(
            sentence_vector_records,
            label,
        )
        duplicate_sentence_count = len(sentence_vector_records) - len(classifications)
        self.logger.info(
            f"{label} completed: {generated_count} new key(s), "
            f"{reused_count} reused key(s); catalog keys: {bedrock_count} "
            f"created with Bedrock, {existing_count} reused; "
            f"{duplicate_sentence_count} duplicate occurrence(s) reused an "
            f"already handled sentence key; {occurrence_count} occurrence(s) "
            "indexed."
        )
        return len(sentence_vector_records)

    def create_catalog_and_classify_new_sentence_batch(
        self,
        sentence_vector_records: list[SentencesVector],
        label: str,
    ) -> dict[str, Classification]:
        """Create catalog vectors and classify unsaved live sentences.

        Input: 30 in-memory sentence records created after document splitting.
        Output: classifications mapped by sentence key. No sentences_vector
        record or occurrence document is written by this method.
        """

        document = sentence_vector_records[0].tsp
        catalog_alias = self.select_catalog_alias(document.analysis_group)
        existing_count, _, bedrock_count = self._ensure_catalog_documents(
            catalog_alias,
            sentence_vector_records,
            {},
            label,
        )
        classifications = self._classify_sentence_batch(
            catalog_alias,
            sentence_vector_records,
            label,
        )
        self._apply_classifications(sentence_vector_records, classifications)
        duplicate_sentence_count = len(sentence_vector_records) - len(classifications)
        self.logger.info(
            f"{label} prepared: catalog keys: {bedrock_count} created with "
            f"Bedrock, {existing_count} reused; {duplicate_sentence_count} "
            "duplicate occurrence(s) reused an already handled sentence key."
        )
        return classifications

    def index_sentence_occurrence_batch(
        self,
        sentence_vector_records: list[SentencesVector],
        label: str,
    ) -> int:
        """Save occurrences after live sentences receive global IDs.

        Input: 30 saved sentence records with global IDs and classifications.
        Output: 30 after their occurrence documents are indexed.
        """

        return self._index_occurrence_documents(sentence_vector_records, label)

    @staticmethod
    def select_catalog_alias(analysis_group: str) -> str:
        """Return the sentence catalog alias for one analysis group.

        Input: analysis_group="Asylee"
        Output: "sentence_semantic_catalog_asylee"

        Raises:
            PermanentDataError: If the analysis group is unsupported.
        """

        normalized_group = analysis_group.strip().casefold()
        if normalized_group == "asylee":
            return constants.ASYLEE_CATALOG_ALIAS
        if normalized_group == "refugee":
            return constants.REFUGEE_CATALOG_ALIAS
        raise PermanentDataError(f"unsupported analysis_group {analysis_group!r}")

    def _ensure_catalog_documents(
        self,
        catalog_alias: str,
        sentence_vector_records: list[SentencesVector],
        embeddings: dict[int, list[float]],
        label: str,
    ) -> tuple[int, int, int]:
        """Save only sentence keys that are missing from the selected catalog.

        Input: 30 SentencesVector records where 5 keys exist, 10 have saved
        vectors, and 15 require Bedrock vectors.
        Output: (5, 10, 15), meaning 5 existing keys, 10 saved database
        vectors, and 15 vectors created through the Bedrock pipeline.

        Duplicate sentence keys inside the same worker batch are written only
        once. Every occurrence is saved later, even when its catalog key was a
        duplicate or already existed.
        """

        existing_keys = retry_operation(
            f"Check existing catalog keys for {label}",
            self.sentence_catalog_opensearch_store.find_existing_sentence_keys,
            catalog_alias,
            [
                sentence_vector_record.sentence_key
                for sentence_vector_record in sentence_vector_records
            ],
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        documents_with_vectors, documents_needing_vectors = (
            self._prepare_catalog_documents(
                sentence_vector_records,
                existing_keys,
                embeddings,
            )
        )

        if documents_with_vectors:
            retry_operation(
                f"Write saved vectors to {catalog_alias} for {label}",
                self.sentence_catalog_opensearch_store.index_saved_vectors,
                catalog_alias,
                documents_with_vectors,
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=1,
                custom_logger=self.logger,
            )
        if documents_needing_vectors:
            retry_operation(
                f"Create Bedrock vectors in {catalog_alias} for {label}",
                self.sentence_catalog_opensearch_store.index_with_bedrock,
                catalog_alias,
                documents_needing_vectors,
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=1,
                custom_logger=self.logger,
            )
        return (
            len(existing_keys),
            len(documents_with_vectors),
            len(documents_needing_vectors),
        )

    def _classify_sentence_batch(
        self,
        catalog_alias: str,
        sentence_vector_records: list[SentencesVector],
        label: str,
    ) -> dict[str, Classification]:
        """Return a classification for every unique sentence key.

        Input: 30 sentence records containing 25 unique keys.
        Output: 25 key classifications after missing values are found through
        the tracer catalog and cached in the sentence catalog.
        """

        sentence_keys = list(
            dict.fromkeys(
                record.sentence_key for record in sentence_vector_records
            )
        )
        catalog_records = retry_operation(
            f"Load catalog vectors for {label}",
            self.sentence_catalog_opensearch_store.load_catalog_records_by_sentence_keys,
            catalog_alias,
            sentence_keys,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        missing_keys = [key for key in sentence_keys if key not in catalog_records]
        if missing_keys:
            raise PermanentDataError(
                "catalog vector is missing for sentence key(s): "
                + ", ".join(missing_keys)
            )

        analysis_group = sentence_vector_records[0].tsp.analysis_group
        classifications = (
            self.tracer_filtering_service.classify_with_tracer_and_save_to_sentence_catalog(
                catalog_alias,
                catalog_records,
                analysis_group,
                label,
            )
        )
        return classifications

    @staticmethod
    def _apply_classifications(
        sentence_vector_records: list[SentencesVector],
        classifications: dict[str, Classification],
    ) -> None:
        """Copy key classifications onto in-memory sentence records."""

        for sentence_vector_record in sentence_vector_records:
            classification = classifications[sentence_vector_record.sentence_key]
            sentence_vector_record.is_non_matcher = classification["is_non_matcher"]
            sentence_vector_record.is_tracer = classification["is_tracer"]
            sentence_vector_record.is_form_language = classification[
                "is_form_language"
            ]

    def _prepare_catalog_documents(
        self,
        sentence_vector_records: list[SentencesVector],
        existing_keys: set[str],
        embeddings: dict[int, list[float]],
    ) -> tuple[list[CatalogDocument], list[CatalogDocument]]:
        """Build unique catalog documents and separate them by vector source.

        Input: sentence records, keys already stored in the catalog, and saved
        Titan vectors mapped by global ID.
        Output: documents with reusable vectors and documents whose vectors
        must be created by the OpenSearch embedding pipeline.

        Existing catalog keys and duplicate keys in this batch are skipped. A
        missing or invalid saved vector is left off the document so the
        OpenSearch pipeline can create a replacement vector.
        """

        documents_with_vectors: list[CatalogDocument] = []
        documents_needing_vectors: list[CatalogDocument] = []
        seen_sentence_keys: set[str] = set()

        for sentence_vector_record in sentence_vector_records:
            sentence_key = sentence_vector_record.sentence_key
            if sentence_key in existing_keys or sentence_key in seen_sentence_keys:
                continue

            seen_sentence_keys.add(sentence_key)
            catalog_document: CatalogDocument = {
                "_id": sentence_key,
                "sentenceKey": sentence_key,
                "sentenceContent": sentence_vector_record.sentence_content,
                "wordCount": sentence_word_count(
                    sentence_vector_record.sentence_content
                ),
                "createdAt": sentence_vector_record.last_modified.isoformat(),
                "updatedAt": sentence_vector_record.last_modified.isoformat(),
            }
            saved_vector = embeddings.get(sentence_vector_record.global_id)
            if saved_vector is None:
                documents_needing_vectors.append(catalog_document)
                continue

            try:
                self.opensearch_helper.validate_vector(saved_vector)
            except PermanentDataError as error:
                # Do not reuse an old vector with the wrong dimension. Leaving
                # it out lets the OpenSearch pipeline create a valid vector.
                self.logger.warning(
                    "Saved Titan embedding is invalid for "
                    f"global_id={sentence_vector_record.global_id}; OpenSearch "
                    "will create a replacement vector. Cause: "
                    f"{safe_error_summary(error)}"
                )
                documents_needing_vectors.append(catalog_document)
                continue

            catalog_document["sentenceContentVector"] = saved_vector
            documents_with_vectors.append(catalog_document)

        return documents_with_vectors, documents_needing_vectors

    def _index_occurrence_documents(
        self,
        sentence_vector_records: list[SentencesVector],
        label: str,
    ) -> int:
        """Build and save every occurrence document for one sentence batch.

        Input: 30 SentencesVector records for one document.
        Output: 30 after all occurrence documents are saved in OpenSearch.
        """

        occurrences = self.sentence_occurrence_opensearch_store.build_documents(
            sentence_vector_records
        )
        retry_operation(
            f"Write occurrences for {label}",
            self.sentence_occurrence_opensearch_store.index_occurrences,
            occurrences,
            wait_seconds=constants.RETRY_SECONDS,
            max_attempts=1,
            custom_logger=self.logger,
        )
        return len(occurrences)

