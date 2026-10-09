from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.contracts import Classification
from similarity_completed.opensearch.utill.constants import VECTOR_FIELD
from similarity_completed.opensearch.utill.score import cosine_similarity
from similarity_completed.opensearch.utill.vector import validate_vector

# Search the 100 closest tracer references. Only results meeting the configured
# 70% similarity threshold contribute to the sentence classification.
TRACER_TOP_K = 100


class TracerOpenSearchStore:
    def __init__(self, config: Config, client) -> None:
        """Keep OpenSearch operations for the tracer/form-language catalog."""

        self.config = config
        self.client = client

    def classify_sentence_by_vector(self, vector: list[float], analysis_group: str) -> Classification:
        """Classify one sentence using matching tracer references.

        Input: analysis_group=Asylee and vector=[512 Titan values]
        Output: flags combined from every top-K result meeting the configured
        70% similarity threshold. Different hits can make all flags true.
        """

        validate_vector(vector, self.config.vector_dimension)
        response = self.client.search(
            index=self.config.tracer_alias,
            body={
                "size": TRACER_TOP_K,
                "track_total_hits": False,
                "_source": ["isNonMatcher", "isFormLanguage"],
                "query": {
                    "bool": {
                        "filter": [{"term": {"analysisGroup": analysis_group}}],
                        "must": [{"knn": {VECTOR_FIELD: {"vector": vector, "k": TRACER_TOP_K}}}],
                    }
                },
            },
        )
        minimum_similarity = self.config.tracer_threshold / 100.0
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
