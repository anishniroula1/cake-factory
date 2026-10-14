from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import Classification
from similarity_completed.opensearch.utill.score import cosine_similarity
from similarity_completed.opensearch.utill.vector import validate_vector


class TracerOpenSearchStore:
    """Classify sentence vectors against tracer and form-language reference records."""

    def __init__(self, client) -> None:
        self.client = client

    def classify_sentence_by_vector(self, vector: list[float], analysis_group: str) -> Classification:
        """Derive classification flags from qualifying tracer-catalog hits. The
        100 closest references are checked, and every qualifying type contributes
        to the result.

        Input: vector=[512 Titan float values], analysis_group="Asylee"
        Output: {"is_non_matcher": True, "is_tracer": False,
        "is_form_language": True}

        All three flags are False when no tracer-catalog hit reaches the
        configured classification threshold.
        """

        validate_vector(vector, constants.VECTOR_DIMENSION)
        response = self.client.search(
            index=constants.TRACER_ALIAS,
            body={
                "size": constants.TRACER_TOP_K,
                "track_total_hits": False,
                "_source": ["isNonMatcher", "isFormLanguage"],
                "query": {
                    "bool": {
                        "filter": [{"term": {"analysisGroup": analysis_group}}],
                        "must": [
                            {
                                "knn": {
                                    constants.VECTOR_FIELD: {"vector": vector, "k": constants.TRACER_TOP_K}
                                }
                            }
                        ],
                    }
                },
            },
        )
        minimum_similarity = constants.TRACER_THRESHOLD / 100.0
        qualifying_hits = [
            hit for hit in response["hits"]["hits"] if cosine_similarity(float(hit["_score"])) >= minimum_similarity
        ]
        if not qualifying_hits:
            return {"is_non_matcher": False, "is_tracer": False, "is_form_language": False}

        # Each qualifying result contributes its own type. For example, one
        # hit can be a tracer, a second can be a non-matcher, and a third can
        # be form language. In that case all three returned flags are true.
        sources = [hit.get("_source", {}) for hit in qualifying_hits]
        is_non_matcher = any(bool(source.get("isNonMatcher")) for source in sources)
        is_form_language = any(bool(source.get("isFormLanguage")) for source in sources)
        is_tracer = any(
            not bool(source.get("isNonMatcher")) and not bool(source.get("isFormLanguage")) for source in sources
        )
        return {"is_non_matcher": is_non_matcher, "is_tracer": is_tracer, "is_form_language": is_form_language}
