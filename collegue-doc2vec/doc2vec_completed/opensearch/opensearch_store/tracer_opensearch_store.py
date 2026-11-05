from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.contracts import Classification, TracerDocument
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

    def search_classification_by_vector(
        self,
        vector: list[float],
        analysis_group: str,
    ) -> Classification:
        """Classify one sentence from the closest qualifying tracer records.

        Input: one 512-value sentence vector and analysis_group="Asylee".
        Output: all tracer, non-matcher, and form-language flags found at or
        above the configured 70 percent threshold.
        """

        self.opensearch_helper.validate_vector(vector)
        response = self.opensearch_helper.search(
            self.alias,
            {
                "size": constants.TRACER_TOP_K,
                "track_total_hits": False,
                "_source": ["isNonMatcher", "isFormLanguage"],
                "query": {
                    "bool": {
                        "filter": [{"term": {"analysisGroup": analysis_group}}],
                        "must": [
                            {
                                "knn": {
                                    constants.VECTOR_FIELD: {
                                        "vector": vector,
                                        "k": constants.TRACER_TOP_K,
                                    }
                                }
                            }
                        ],
                    }
                },
            },
        )
        minimum_similarity = constants.TRACER_THRESHOLD / 100.0
        qualifying_sources = [
            hit.get("_source", {})
            for hit in response.get("hits", {}).get("hits", [])
            if self._cosine_similarity(float(hit["_score"])) >= minimum_similarity
        ]
        return {
            "is_non_matcher": any(
                bool(source.get("isNonMatcher")) for source in qualifying_sources
            ),
            "is_form_language": any(
                bool(source.get("isFormLanguage")) for source in qualifying_sources
            ),
            "is_tracer": any(
                not bool(source.get("isNonMatcher"))
                and not bool(source.get("isFormLanguage"))
                for source in qualifying_sources
            ),
        }

    @staticmethod
    def _cosine_similarity(opensearch_score: float) -> float:
        """Convert an OpenSearch cosine score into decimal similarity.

        Input: score=0.85
        Output: 0.70, which is a 70 percent vector similarity.
        """

        return round(max(-1.0, min(1.0, (2.0 * opensearch_score) - 1.0)), 8)


