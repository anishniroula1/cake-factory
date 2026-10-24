from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import TracerDocument
from doc2vec_completed.opensearch.open_search_helper import OpenSearchHelper


class TracerOpenSearchStore:
    """Write tracer, form-language, and non-matcher reference documents."""

    def __init__(
        self,
        config: Config,
        opensearch_helper: OpenSearchHelper,
    ) -> None:
        self.alias = config.tracer_alias
        self.opensearch_helper = opensearch_helper

    def index_saved_vectors(self, documents: list[TracerDocument]) -> int:
        """Index tracer documents that already contain Titan vectors."""

        return self.opensearch_helper.index_documents(
            self.alias,
            documents,
            needs_embedding=False,
        )

    def index_with_bedrock(self, documents: list[TracerDocument]) -> int:
        """Index tracer documents and let the pipeline create their vectors."""

        return self.opensearch_helper.index_documents(
            self.alias,
            documents,
            needs_embedding=True,
        )

    def validate_vector(self, vector: list[float]) -> None:
        """Validate one saved Titan vector before tracer indexing."""

        self.opensearch_helper.validate_vector(vector)

    def refresh(self) -> None:
        """Make newly indexed tracer documents searchable."""

        self.opensearch_helper.refresh_aliases([self.alias])
