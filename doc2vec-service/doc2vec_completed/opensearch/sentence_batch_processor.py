from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import bindparam, select, update
from sqlalchemy.orm import Session

from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import CatalogDocument, OccurrenceDocument
from doc2vec_completed.opensearch.custom_logger import CustomLogger
from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.models import Embeddings, SentencesVector
from doc2vec_completed.opensearch.opensearch import OpenSearchHelper
from doc2vec_completed.opensearch.text import sentence_key


class SentenceBatchProcessor:
    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch: OpenSearchHelper,
        custom_logger: CustomLogger,
    ) -> None:
        """Keep the resources used to index one reusable sentence batch."""

        self.config = config
        self.session_factory = session_factory
        self.opensearch = opensearch
        self.logger = custom_logger

    def process(self, rows: list[SentencesVector], label: str) -> int:
        """Prepare and index one sentence batch.

        Input: rows contains one document's sentences and label identifies the
        batch in logs. Output: number of indexed occurrence documents.
        """

        stage = "sentence-key update"
        try:
            self.logger.info(f"{label} is preparing sentence keys.")
            generated_count, reused_count = self._save_keys(rows)
            self.logger.info(
                f"{label} prepared sentence keys. Generated {generated_count} "
                f"new key(s) and reused {reused_count} existing key(s)."
            )

            stage = "catalog selection"
            document = rows[0].tsp
            catalog_alias = self._catalog_alias(document.analysis_group)
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

    def _save_keys(self, rows: list[SentencesVector]) -> tuple[int, int]:
        """Reuse existing keys and save only missing keys.

        Output: (generated key count, reused key count).
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
            # Restore the in-memory rows so a retry does not mistake an
            # uncommitted generated key for one that already exists.
            for row, original_key, original_last_modified in prepared_rows:
                row.sentence_key = original_key
                row.last_modified = original_last_modified
            raise
        return len(updates), reused_count

    def _catalog_alias(self, analysis_group: str) -> str:
        """Return the catalog selected for one analysis group."""

        normalized_group = analysis_group.strip().casefold()
        if normalized_group == "asylee":
            return self.config.asylee_catalog_alias
        if normalized_group == "refugee":
            return self.config.refugee_catalog_alias
        raise PermanentDataError(f"unsupported analysis_group {analysis_group!r}")

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

    def _index_catalog_rows(
        self,
        catalog_alias: str,
        rows: list[SentencesVector],
        embeddings: dict[int, list[float]],
        label: str,
    ) -> None:
        """Write missing unique keys to one analysis-group catalog."""

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

        self.logger.info(
            f"{label} is writing catalog {catalog_alias}. Existing keys: "
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
            f"{label} completed catalog {catalog_alias} with "
            f"{len(with_vectors) + len(need_vectors)} indexed document(s)."
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
