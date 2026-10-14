# OpenSearch cosine distance and score formulas:
# https://docs.opensearch.org/latest/mappings/supported-field-types/knn-spaces/


def minimum_opensearch_score(threshold: int) -> float:
    """Convert the requested match percentage into an OpenSearch score.

    Input: threshold=90
    Output: 0.95, the minimum _score sent to the OpenSearch query.

    OpenSearch changes cosine similarity into its own positive score. This
    means 90% similarity uses _score=0.95, while 100% similarity uses
    _score=1.0.
    """

    return (1.0 + (threshold / 100.0)) / 2.0


def cosine_distance(opensearch_score: float) -> float:
    """Convert an OpenSearch score to the distance stored in titan_matches.

    Input: opensearch_score=0.95
    Output: 0.10, which means the sentences are 90% similar.

    Input: opensearch_score=1.0
    Output: 0.0, which means a 100% cosine/vector match. The current matching
    flow treats this distance as exact/verbatim.

    A smaller distance is a better match. Distance 0.0 is exact, distance
    0.10 is 90% similar, and distance 0.24 is 76% similar.
    """

    return round(1.0 - cosine_similarity(opensearch_score), 8)


def cosine_similarity(opensearch_score: float) -> float:
    """Convert an OpenSearch score into decimal cosine similarity.

    Input: opensearch_score=0.95
    Output: 0.90, which means the sentences are 90% similar.

    Input: opensearch_score=1.0
    Output: 1.0, which means the sentences are a 100% match.

    OpenSearch _score is not the percentage itself. The calculation is
    similarity = (2 * _score) - 1.

    The result is kept between -1.0 and 1.0 so an unusual OpenSearch score
    cannot produce an invalid cosine similarity.
    """

    return round(max(-1.0, min(1.0, (2.0 * opensearch_score) - 1.0)), 8)
