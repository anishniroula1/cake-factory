from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import CatalogDocument, OccurrenceDocument
from doc2vec_completed.opensearch.db_store import (
    EmbeddingDBStore,
    SentenceVectorDBStore,
)
from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.opensearch_store import OpenSearchDocumentStore
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.text import sentence_word_count
from ta_db_connection.atables import SentencesVector
from ta_utils.logger.CustomLogger import CustomLogger


class SentenceBatchProcessor:
    """Create sentence keys and write catalog and occurrence documents for one batch."""

    def __init__(
        self,
        config: Config,
        sentence_vector_db_store: SentenceVectorDBStore,
        embedding_db_store: EmbeddingDBStore,
        opensearch_store: OpenSearchDocumentStore,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.sentence_vector_db_store = sentence_vector_db_store
        self.embedding_db_store = embedding_db_store
        self.opensearch_store = opensearch_store
        self.logger = custom_logger

    def process(self, rows: list[SentencesVector], label: str) -> int:
        """Save sentence keys, unique catalog records, and occurrence documents
        for one worker batch.

        Input: rows=[30 SentencesVector rows], label="global IDs 3001 through 3030"
        Output: 30 after all 30 occurrence documents are written.
        """

        generated_count, reused_count = retry_operation(
            f"Save sentence keys for {label}",
            self.sentence_vector_db_store.save_missing_sentence_keys,
            rows,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )

        document = rows[0].tsp
        catalog_alias = self.catalog_alias(document.analysis_group)
        embeddings = retry_operation(
            f"Load saved embeddings for {label}",
            self.embedding_db_store.load_titan_embeddings_by_global_ids,
            [row.global_id for row in rows],
            self.config.titan_embedding_type,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        existing_count, saved_vector_count, bedrock_count = self._index_catalog_rows(
            catalog_alias,
            rows,
            embeddings,
            label,
        )

        occurrences = self._occurrence_documents(rows)
        retry_operation(
            f"Write occurrences for {label}",
            self.opensearch_store.index_documents,
            self.config.occurrence_alias,
            occurrences,
            False,
            wait_seconds=self.config.retry_seconds,
            max_attempts=self.config.opensearch_bulk_retry_count + 1,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"{label} completed: {generated_count} new key(s), "
            f"{reused_count} reused key(s), {existing_count} existing catalog "
            f"key(s), {saved_vector_count} saved vector(s), {bedrock_count} "
            f"Bedrock vector(s), and {len(occurrences)} occurrence(s)."
        )
        return len(rows)

    def catalog_alias(self, analysis_group: str) -> str:
        """Select the sentence catalog for the document analysis group.

        Input: analysis_group="Asylee"
        Output: "sentence_semantic_catalog_asylee"

        Raises:
            PermanentDataError: If the analysis group is unsupported.
        """

        normalized_group = analysis_group.strip().casefold()
        if normalized_group == "asylee":
            return self.config.asylee_catalog_alias
        if normalized_group == "refugee":
            return self.config.refugee_catalog_alias
        raise PermanentDataError(f"unsupported analysis_group {analysis_group!r}")

    def _index_catalog_rows(
        self,
        catalog_alias: str,
        rows: list[SentencesVector],
        embeddings: dict[int, list[float]],
        label: str,
    ) -> tuple[int, int, int]:
        """Add only missing sentence keys to the selected catalog. Saved Titan
        vectors are reused, while the embedding pipeline creates missing vectors.

        Input: 30 sentence rows where 5 keys exist, 10 have saved vectors, and
        15 require Bedrock vectors.
        Output: (5, 10, 15)
        """

        existing_keys = retry_operation(
            f"Check existing catalog keys for {label}",
            self.opensearch_store.find_existing_document_ids,
            catalog_alias,
            [row.sentence_key for row in rows],
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
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
                "wordCount": sentence_word_count(row.sentence_content),
                "createdAt": row.last_modified.isoformat(),
                "updatedAt": row.last_modified.isoformat(),
            }
            vector = embeddings.get(row.global_id)
            if vector is None:
                need_vectors.append(document)
                continue
            self.opensearch_store.validate_vector(vector)
            document["sentenceContentVector"] = vector
            with_vectors.append(document)

        retry_operation(
            f"Write saved vectors to {catalog_alias} for {label}",
            self.opensearch_store.index_documents,
            catalog_alias,
            with_vectors,
            False,
            wait_seconds=self.config.retry_seconds,
            max_attempts=self.config.opensearch_bulk_retry_count + 1,
            custom_logger=self.logger,
        )
        retry_operation(
            f"Create Bedrock vectors in {catalog_alias} for {label}",
            self.opensearch_store.index_documents,
            catalog_alias,
            need_vectors,
            True,
            wait_seconds=self.config.retry_seconds,
            max_attempts=self.config.opensearch_bulk_retry_count + 1,
            custom_logger=self.logger,
        )
        return len(existing_keys), len(with_vectors), len(need_vectors)

    def _occurrence_documents(
        self,
        rows: list[SentencesVector],
    ) -> list[OccurrenceDocument]:
        """Build one occurrence document for every sentence row. Sentence fields
        come from SentencesVector, while application fields come from its tsp row.

        Input: rows=[SentencesVector(global_id=10, sentence_key="abc123", ...)]
        Output: [{"_id": 10, "globalId": 10, "sentenceKey": "abc123", ...}]
        """

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
