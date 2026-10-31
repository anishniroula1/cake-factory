from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.contracts import TracerDocument
from doc2vec_completed.opensearch.opensearch_helper import OpenSearchHelper


class TracerOpenSearchStore:
    """Write tracer, form-language, and non-matcher reference documents."""

    def __init__(
        self,
        opensearch_helper: OpenSearchHelper,
    ) -> None:
        self.alias = constants.TRACER_ALIAS
        self.opensearch_helper = opensearch_helper

    def index_saved_vectors(self, documents: list[TracerDocument]) -> int:
        """Index tracer documents that already contain Titan vectors.

        Input: tracer documents containing sentenceContentVector.
        Output: the number accepted without using the embedding pipeline.
        """

        return self.opensearch_helper.index_documents(
            self.alias,
            documents,
            needs_embedding=False,
        )

    def index_with_bedrock(self, documents: list[TracerDocument]) -> int:
        """Index tracer documents and let the pipeline create their vectors.

        Input: tracer documents without sentenceContentVector.
        Output: the number accepted by the embedding pipeline.
        """

        return self.opensearch_helper.index_documents(
            self.alias,
            documents,
            needs_embedding=True,
        )

    def validate_vector(self, vector: list[float]) -> None:
        """Validate one saved Titan vector before tracer indexing.

        Input: vector containing 512 float values.
        """

        self.opensearch_helper.validate_vector(vector)


