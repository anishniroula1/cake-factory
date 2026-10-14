from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import bindparam, select, update
from sqlalchemy.orm import Session

from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import CatalogDocument, OccurrenceDocument
from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.opensearch import OpenSearchHelper
from doc2vec_completed.opensearch.retry import retry_operation
from doc2vec_completed.opensearch.text import sentence_key
from ta_db_connection.atables import Embeddings, SentencesVector
from ta_utils.logger.CustomLogger import CustomLogger


class SentenceBatchProcessor:
    """Create sentence keys and write catalog and occurrence documents for one batch."""

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

    def process(self, rows: list[SentencesVector], label: str) -> int:
        """Save sentence keys, unique catalog records, and occurrence documents
        for one worker batch.

        Input: rows=[30 SentencesVector rows], label="global IDs 3001 through 3030"
        Output: 30 after all 30 occurrence documents are written.
        """

        generated_count, reused_count = retry_operation(
            f"Save sentence keys for {label}",
            self._save_keys,
            rows,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )

        document = rows[0].tsp
        catalog_alias = self.catalog_alias(document.analysis_group)
        embeddings = retry_operation(
            f"Load saved embeddings for {label}",
            self._load_embeddings,
            [row.global_id for row in rows],
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
            self.opensearch.bulk_index,
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

    def _save_keys(self, rows: list[SentencesVector]) -> tuple[int, int]:
        """Reuse saved sentence keys and generate only missing values.

        Input: 30 rows where 10 already contain sentence_key.
        Output: (20, 10), meaning 20 generated keys and 10 reused keys.
        """

        current_time = datetime.now(UTC)
        updates = []
        prepared_rows = []
        reused_count = 0
        for row in rows:
            if row.sentence_key:
                reused_count += 1
                continue

            prepared_rows.append((row, row.sentence_key, row.last_modified))
            row.sentence_key = sentence_key(row.sentence_content)
            row.last_modified = current_time
            updates.append(
                {
                    "record_global_id": row.global_id,
                    "record_sentence_key": row.sentence_key,
                    "record_last_modified": current_time,
                }
            )

        if not updates:
            return 0, reused_count

        table = SentencesVector.__table__
        statement = (
            update(table)
            .where(table.c.global_id == bindparam("record_global_id"))
            .values(
                sentence_key=bindparam("record_sentence_key"),
                last_modified=bindparam("record_last_modified"),
            )
        )
        try:
            with self.session_factory() as session:
                session.execute(statement, updates)
                session.commit()
        except Exception:
            # The database did not save these keys. Put the Python rows back the
            # way they were so the retry creates and saves the keys again.
            for row, original_key, original_last_modified in prepared_rows:
                row.sentence_key = original_key
                row.last_modified = original_last_modified
            raise
        return len(updates), reused_count

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

    def _load_embeddings(self, global_ids: list[int]) -> dict[int, list[float]]:
        """Read saved Titan vectors for the requested global IDs.

        Input: global_ids=[10, 20]
        Output: {10: [512 vector values]} when only global ID 10 has a saved
        Titan embedding with embedding_type=2.
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
            self.opensearch.existing_ids,
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

        retry_operation(
            f"Write saved vectors to {catalog_alias} for {label}",
            self.opensearch.bulk_index,
            catalog_alias,
            with_vectors,
            False,
            wait_seconds=self.config.retry_seconds,
            max_attempts=self.config.opensearch_bulk_retry_count + 1,
            custom_logger=self.logger,
        )
        retry_operation(
            f"Create Bedrock vectors in {catalog_alias} for {label}",
            self.opensearch.bulk_index,
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
