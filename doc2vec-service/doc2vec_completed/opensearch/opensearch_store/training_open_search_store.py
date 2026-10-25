from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import TrainingDocument
from doc2vec_completed.opensearch.open_search_helper import OpenSearchHelper


class TrainingOpenSearchStore:
    """Read and write documents used to train the IVF model."""

    def __init__(
        self,
        config: Config,
        opensearch_helper: OpenSearchHelper,
    ) -> None:
        self.alias = constants.TRAINING_ALIAS
        self.opensearch_helper = opensearch_helper

    def find_existing_sentence_keys(self, sentence_keys: list[str]) -> set[str]:
        """Return training sentence keys that are already indexed."""

        return self.opensearch_helper.find_existing_document_ids(
            self.alias,
            sentence_keys,
        )

    def index_saved_vectors(self, documents: list[TrainingDocument]) -> int:
        """Index training documents that already contain Titan vectors."""

        return self.opensearch_helper.index_documents(
            self.alias,
            documents,
            needs_embedding=False,
        )

    def index_with_bedrock(self, documents: list[TrainingDocument]) -> int:
        """Index training documents and let the pipeline create their vectors."""

        return self.opensearch_helper.index_documents(
            self.alias,
            documents,
            needs_embedding=True,
        )

    def validate_vector(self, vector: list[float]) -> None:
        """Validate one saved Titan vector before training-index insertion."""

        self.opensearch_helper.validate_vector(vector)

    def refresh(self) -> None:
        """Make newly indexed training documents searchable."""

        self.opensearch_helper.refresh_aliases([self.alias])
