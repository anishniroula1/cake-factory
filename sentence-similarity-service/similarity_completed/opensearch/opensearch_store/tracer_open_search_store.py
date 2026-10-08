from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.contracts import Classification
from similarity_completed.opensearch.errors import PermanentDataError
from similarity_completed.opensearch.tracer_filtering_service import (
    TracerFilteringService,
)
from similarity_completed.opensearch.utill.constants import VECTOR_FIELD
from similarity_completed.opensearch.utill.score import (
    cosine_similarity,
    minimum_opensearch_score,
)
from similarity_completed.opensearch.utill.vector import validate_vector

# The tracer catalog currently has about 4,000 rows. This size returns every
# reference meeting the threshold without needing another result page.
TRACER_RESULT_SIZE = 10_000


class TracerOpenSearchStore:
    def __init__(self, config: Config, client) -> None:
        """Keep OpenSearch operations for the tracer/form-language catalog."""

        self.config = config
        self.client = client

    def classify_sentence_by_vector(
        self,
        vector: list[float],
        analysis_group: str,
    ) -> Classification:
        """Classify one sentence using matching tracer references.

        Input: analysis_group=Asylee and vector=[512 Titan values]
        Output: the closest reference and the combined classification flags.
        """

        validate_vector(vector, self.config.vector_dimension)
        response = self.client.search(
            index=self.config.tracer_alias,
            body={
                "size": TRACER_RESULT_SIZE,
                "track_total_hits": False,
                "_source": [
                    "tracerId",
                    "isNonMatcher",
                    "isFormLanguage",
                ],
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"analysisGroup": analysis_group}},
                        ],
                        "must": [
                            {
                                "knn": {
                                    VECTOR_FIELD: {
                                        "vector": vector,
                                        "min_score": minimum_opensearch_score(
                                            self.config.tracer_threshold
                                        ),
                                    }
                                }
                            }
                        ],
                    }
                },
            },
        )
        hits = response["hits"]["hits"]
        if not hits:
            return {
                "is_non_matcher": False,
                "closest_non_matcher": None,
                "closest_nn_score": None,
                "is_tracer": False,
                "is_form_language": False,
            }

        # Hits are ordered highest score first. The closest hit supplies the
        # reference ID and score. Every hit contributes isNonMatcher and
        # isFormLanguage. The tracer table has no isTracer field, so the
        # filtering helper derives it from those two flags.
        closest_hit = hits[0]
        closest_source = closest_hit.get("_source", {})
        try:
            closest_non_matcher = int(closest_source["tracerId"])
            closest_nn_score = cosine_similarity(float(closest_hit["_score"]))
        except (KeyError, TypeError, ValueError) as error:
            raise PermanentDataError(
                "OpenSearch tracer result is missing tracerId or _score"
            ) from error

        is_non_matcher = any(
            bool(hit.get("_source", {}).get("isNonMatcher")) for hit in hits
        )
        is_form_language = any(
            bool(hit.get("_source", {}).get("isFormLanguage")) for hit in hits
        )
        return {
            "is_non_matcher": is_non_matcher,
            "closest_non_matcher": closest_non_matcher,
            "closest_nn_score": closest_nn_score,
            "is_tracer": TracerFilteringService.derive_is_tracer(
                tracer_match_found=True,
                is_non_matcher=is_non_matcher,
                is_form_language=is_form_language,
            ),
            "is_form_language": is_form_language,
        }
