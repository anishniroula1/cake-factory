from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import CatalogDocument
from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.open_search_helper import OpenSearchHelper


class SentenceCatalogOpenSearchStore:
    """Read and write unique sentences in the Asylee and Refugee catalogs."""

    def __init__(
        self,
        config: Config,
        opensearch_helper: OpenSearchHelper,
    ) -> None:
        self.config = config
        self.opensearch_helper = opensearch_helper

    def select_alias(self, analysis_group: str) -> str:
        """Return the catalog alias belonging to an analysis group.

        Input: analysis_group="Asylee"
        Output: "sentence_semantic_catalog_asylee"
        """

        normalized_group = analysis_group.strip().casefold()
        if normalized_group == "asylee":
            return self.config.asylee_catalog_alias
        if normalized_group == "refugee":
            return self.config.refugee_catalog_alias
        raise PermanentDataError(f"unsupported analysis_group {analysis_group!r}")

    def find_existing_sentence_keys(
        self,
        alias: str,
        sentence_keys: list[str],
    ) -> set[str]:
        """Return requested sentence keys already stored in one catalog."""

        return self.opensearch_helper.find_existing_document_ids(
            alias,
            sentence_keys,
        )

    def index_saved_vectors(
        self,
        alias: str,
        documents: list[CatalogDocument],
    ) -> int:
        """Index catalog documents that already contain Titan vectors."""

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
        """Index catalog documents and let the pipeline create their vectors."""

        return self.opensearch_helper.index_documents(
            alias,
            documents,
            needs_embedding=True,
        )

    def validate_vector(self, vector: list[float]) -> None:
        """Validate one saved Titan vector before catalog indexing."""

        self.opensearch_helper.validate_vector(vector)
