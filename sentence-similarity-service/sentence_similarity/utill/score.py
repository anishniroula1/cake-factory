# OpenSearch cosine distance and score formulas:
# https://docs.opensearch.org/latest/mappings/supported-field-types/knn-spaces/


def minimum_opensearch_score(threshold: int) -> float:
    """Convert a cosine percentage into the OpenSearch cosine score.

    Input: threshold=90
    Output: 0.95, the minimum _score sent to the OpenSearch query.

    OpenSearch converts cosine similarity into a positive score. A requested
    cosine similarity of 0.90 therefore becomes an OpenSearch score of 0.95.
    """

    return (1.0 + (threshold / 100.0)) / 2.0


def cosine_distance(opensearch_score: float) -> float:
    """Convert an OpenSearch score to distance where zero is exact.

    Input: opensearch_score=0.95
    Output: 0.10, which means the sentences are 90% similar.

    titan_matches stores distance, so an exact match is 0.0 and values become
    larger as sentences become less similar.
    """

    return round(1.0 - cosine_similarity(opensearch_score), 8)


def cosine_similarity(opensearch_score: float) -> float:
    """Convert an OpenSearch score to a decimal similarity value.

    Input: opensearch_score=0.95
    Output: 0.90, which means the sentences are 90% similar.

    The result is kept between -1.0 and 1.0 so an unusual OpenSearch score
    cannot produce an invalid cosine similarity.
    """

    return round(max(-1.0, min(1.0, (2.0 * opensearch_score) - 1.0)), 8)
