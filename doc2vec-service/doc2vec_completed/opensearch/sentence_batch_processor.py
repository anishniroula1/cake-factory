from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import CatalogDocument
from doc2vec_completed.opensearch.db_store import (
    EmbeddingDBStore,
    SentenceVectorDBStore,
)
from doc2vec_completed.opensearch.open_search_helper import OpenSearchHelper
from doc2vec_completed.opensearch.opensearch_store import (
    SentenceCatalogOpenSearchStore,
    SentenceOccurrenceOpenSearchStore,
)
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.text import sentence_word_count
from ta_db_connection.atables import SentencesVector
from ta_utils.logger.CustomLogger import CustomLogger


class SentenceBatchProcessor:
    """Write incoming or historical sentence batches to both OpenSearch indexes."""

    def __init__(
        self,
        config: Config,
        opensearch_helper: OpenSearchHelper,
        custom_logger: CustomLogger,
    ) -> None:
        self.sentence_catalog_open_search_store = SentenceCatalogOpenSearchStore(
            config,
            opensearch_helper,
        )
        self.sentence_occurrence_open_search_store = SentenceOccurrenceOpenSearchStore(
            config, opensearch_helper
        )
        self.logger = custom_logger

    def save_historical_sentence_batch(
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

        The method saves missing sentence keys, reuses Titan vectors stored in
        PostgreSQL, and asks Bedrock to create only the vectors still missing.
        """

        generated_count, reused_count = retry_operation(
            f"Save sentence keys for {label}",
            sentence_vector_db_store.save_missing_sentence_keys,
            sentence_vector_records,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
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
        existing_count, saved_vector_count, bedrock_count = self._save_catalog_documents(
            catalog_alias,
            sentence_vector_records,
            embeddings,
            label,
        )

        occurrence_count = self._save_occurrence_documents(
            sentence_vector_records,
            label,
        )
        self.logger.info(
            f"{label} completed: {generated_count} new key(s), "
            f"{reused_count} reused key(s), {existing_count} existing catalog "
            f"key(s), {saved_vector_count} saved vector(s), {bedrock_count} "
            f"Bedrock vector(s), and {occurrence_count} occurrence(s)."
        )
        return len(sentence_vector_records)

    def save_document_sentence_batch(
        self,
        sentence_vector_records: list[SentencesVector],
        label: str,
    ) -> int:
        """Save one incoming document batch in catalog and occurrence indexes.

        Input: 30 SentencesVector records for global IDs 1 through 30.
        Output: 30 after catalog and occurrence writes complete.

        These records already contain sentence keys. The method checks which
        keys exist, uses Bedrock for missing catalog vectors, and saves an
        occurrence document for every record.
        """

        document = sentence_vector_records[0].tsp
        catalog_alias = self.select_catalog_alias(document.analysis_group)
        existing_count, _, bedrock_count = self._save_catalog_documents(
            catalog_alias,
            sentence_vector_records,
            {},
            label,
        )
        occurrence_count = self._save_occurrence_documents(
            sentence_vector_records,
            label,
        )
        self.logger.info(
            f"{label} completed: {existing_count} existing catalog key(s), "
            f"{bedrock_count} Bedrock vector(s), and "
            f"{occurrence_count} occurrence(s)."
        )
        return len(sentence_vector_records)

    def select_catalog_alias(self, analysis_group: str) -> str:
        """Return the sentence catalog alias for one analysis group.

        Input: analysis_group="Asylee"
        Output: "sentence_semantic_catalog_asylee"

        Raises:
            PermanentDataError: If the analysis group is unsupported.
        """

        return self.sentence_catalog_open_search_store.select_alias(analysis_group)

    def _save_catalog_documents(
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
            self.sentence_catalog_open_search_store.find_existing_sentence_keys,
            catalog_alias,
            [
                sentence_vector_record.sentence_key
                for sentence_vector_record in sentence_vector_records
            ],
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        with_vectors: list[CatalogDocument] = []
        need_vectors: list[CatalogDocument] = []
        seen_keys: set[str] = set()

        for sentence_vector_record in sentence_vector_records:
            sentence_key = sentence_vector_record.sentence_key
            if sentence_key in existing_keys or sentence_key in seen_keys:
                continue
            seen_keys.add(sentence_key)
            document: CatalogDocument = {
                "_id": sentence_key,
                "sentenceKey": sentence_key,
                "sentenceContent": sentence_vector_record.sentence_content,
                "wordCount": sentence_word_count(
                    sentence_vector_record.sentence_content
                ),
                "createdAt": sentence_vector_record.last_modified.isoformat(),
                "updatedAt": sentence_vector_record.last_modified.isoformat(),
            }
            vector = embeddings.get(sentence_vector_record.global_id)
            if vector is None:
                need_vectors.append(document)
                continue
            self.sentence_catalog_open_search_store.validate_vector(vector)
            document["sentenceContentVector"] = vector
            with_vectors.append(document)

        if with_vectors:
            retry_operation(
                f"Write saved vectors to {catalog_alias} for {label}",
                self.sentence_catalog_open_search_store.index_saved_vectors,
                catalog_alias,
                with_vectors,
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=constants.OPENSEARCH_BULK_RETRY_COUNT + 1,
                custom_logger=self.logger,
            )
        if need_vectors:
            retry_operation(
                f"Create Bedrock vectors in {catalog_alias} for {label}",
                self.sentence_catalog_open_search_store.index_with_bedrock,
                catalog_alias,
                need_vectors,
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=constants.OPENSEARCH_BULK_RETRY_COUNT + 1,
                custom_logger=self.logger,
            )
        return len(existing_keys), len(with_vectors), len(need_vectors)

    def _save_occurrence_documents(
        self,
        sentence_vector_records: list[SentencesVector],
        label: str,
    ) -> int:
        """Build and save every occurrence document for one sentence batch.

        Input: 30 SentencesVector records for one document.
        Output: 30 after all occurrence documents are saved in OpenSearch.
        """

        occurrences = self.sentence_occurrence_open_search_store.build_documents(
            sentence_vector_records
        )
        retry_operation(
            f"Write occurrences for {label}",
            self.sentence_occurrence_open_search_store.index_occurrences,
            occurrences,
            wait_seconds=constants.RETRY_SECONDS,
            max_attempts=constants.OPENSEARCH_BULK_RETRY_COUNT + 1,
            custom_logger=self.logger,
        )
        return len(occurrences)
