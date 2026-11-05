import math
from datetime import UTC, datetime

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.contracts import (
    CatalogClassificationDocument,
    CatalogDocument,
    CatalogRecord,
    Classification,
)
from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.opensearch_helper import OpenSearchHelper


class SentenceCatalogOpenSearchStore:
    """Read and write unique sentences in the Asylee and Refugee catalogs."""

    def __init__(
        self,
        opensearch_helper: OpenSearchHelper,
    ) -> None:
        self.opensearch_helper = opensearch_helper

    def find_existing_sentence_keys(
        self,
        alias: str,
        sentence_keys: list[str],
    ) -> set[str]:
        """Return requested sentence keys already stored in one catalog.

        Input: alias="sentence_semantic_catalog_asylee",
        sentence_keys=["key-a", "key-b"] where only key-a exists.
        Output: {"key-a"}
        """

        return self.opensearch_helper.find_existing_document_ids(
            alias,
            sentence_keys,
        )

    def index_saved_vectors(
        self,
        alias: str,
        documents: list[CatalogDocument],
    ) -> int:
        """Index catalog documents that already contain Titan vectors.

        Input: documents=[{"_id": "key-a", "sentenceContentVector": [...]}]
        Output: the number of documents accepted without using the embedding
        pipeline.
        """

        return self.opensearch_helper.index_documents(
            alias,
            documents,
            needs_embedding=False,
        )

    def index_with_bedrock(
        self,
        alias: str,
        documents: list[CatalogDocument],
    ) -> int:
        """Index catalog documents and let the pipeline create their vectors.

        Input: documents=[{"_id": "key-a", "sentenceContent": "..."}]
        Output: the number of documents accepted by the embedding pipeline.
        """

        return self.opensearch_helper.index_documents(
            alias,
            documents,
            needs_embedding=True,
        )

    def load_catalog_records_by_sentence_keys(
        self,
        alias: str,
        sentence_keys: list[str],
    ) -> dict[str, CatalogRecord]:
        """Load vectors and any saved classification by unique sentence key.

        Input: keys ["A", "B", "A"] after catalog indexing finishes.
        Output: one record for A and one for B, including each numeric vector.
        """

        fields = ["sentenceKey", constants.VECTOR_FIELD, *constants.CLASSIFICATION_FIELDS]
        sources = self.opensearch_helper.load_documents_by_ids(
            alias,
            sentence_keys,
            fields,
        )
        records: dict[str, CatalogRecord] = {}
        for document_id, source in sources.items():
            vector = source.get(constants.VECTOR_FIELD)
            if not isinstance(vector, list) or any(
                isinstance(value, bool) or not isinstance(value, (int, float))
                for value in vector
            ):
                raise PermanentDataError(
                    f"catalog vector is invalid for sentence key {document_id}"
                )
            numeric_vector = [float(value) for value in vector]
            self.opensearch_helper.validate_vector(numeric_vector)
            if any(not math.isfinite(value) for value in numeric_vector):
                raise PermanentDataError(
                    f"catalog vector is invalid for sentence key {document_id}"
                )
            records[document_id] = {
                "sentence_key": str(source.get("sentenceKey") or document_id),
                "vector": numeric_vector,
                "classification": self._saved_classification(source),
            }
        return records

    def update_catalog_classifications(
        self,
        alias: str,
        classifications: dict[str, Classification],
    ) -> int:
        """Save tracer classifications without replacing catalog vectors.

        Input: {"A": {"is_non_matcher": True, ...}}
        Output: the number of unique catalog keys updated.
        """

        updated_at = datetime.now(UTC).isoformat()
        documents: list[CatalogClassificationDocument] = [
            {
                "_id": sentence_key,
                "updatedAt": updated_at,
                "isNonMatcher": classification["is_non_matcher"],
                "isTracer": classification["is_tracer"],
                "isFormLanguage": classification["is_form_language"],
            }
            for sentence_key, classification in classifications.items()
        ]
        return self.opensearch_helper.update_documents(alias, documents)

    @staticmethod
    def _saved_classification(source: dict) -> Classification | None:
        """Return a classification only when all three catalog flags exist."""

        if not all(field in source for field in constants.CLASSIFICATION_FIELDS):
            return None
        return {
            "is_non_matcher": bool(source["isNonMatcher"]),
            "is_tracer": bool(source["isTracer"]),
            "is_form_language": bool(source["isFormLanguage"]),
        }


