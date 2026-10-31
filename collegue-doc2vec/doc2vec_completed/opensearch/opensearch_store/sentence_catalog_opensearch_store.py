from doc2vec_completed.opensearch.contracts import CatalogDocument
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

    def validate_vector(self, vector: list[float]) -> None:
        """Validate one saved Titan vector before catalog indexing.

        Input: vector containing 512 float values.
        """

        self.opensearch_helper.validate_vector(vector)

