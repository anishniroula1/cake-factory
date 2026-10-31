from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.contracts import TrainingDocument
from doc2vec_completed.opensearch.opensearch_helper import OpenSearchHelper


class TrainingOpenSearchStore:
    """Read and write documents used to train the IVF model."""

    def __init__(
        self,
        opensearch_helper: OpenSearchHelper,
    ) -> None:
        self.alias = constants.TRAINING_ALIAS
        self.opensearch_helper = opensearch_helper

    def find_existing_sentence_keys(self, sentence_keys: list[str]) -> set[str]:
        """Return training sentence keys that are already indexed.

        Input: sentence_keys=["key-a", "key-b"] where only key-a exists.
        Output: {"key-a"}
        """

        return self.opensearch_helper.find_existing_document_ids(
            self.alias,
            sentence_keys,
        )

    def index_saved_vectors(self, documents: list[TrainingDocument]) -> int:
        """Index training documents that already contain Titan vectors.

        Input: training documents containing sentenceContentVector.
        Output: the number accepted without using the embedding pipeline.
        """

        return self.opensearch_helper.index_documents(
            self.alias,
            documents,
            needs_embedding=False,
        )

    def index_with_bedrock(self, documents: list[TrainingDocument]) -> int:
        """Index training documents and let the pipeline create their vectors.

        Input: training documents without sentenceContentVector.
        Output: the number accepted by the embedding pipeline.
        """

        return self.opensearch_helper.index_documents(
            self.alias,
            documents,
            needs_embedding=True,
        )

