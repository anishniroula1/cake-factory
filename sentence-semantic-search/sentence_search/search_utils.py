import base64
import binascii
import json


def minimum_opensearch_score(threshold: int) -> float:
    """Convert a cosine percentage into an OpenSearch score.

    Input:
        threshold=90
    Output:
        0.95
    """

    if not 1 <= threshold <= 100:
        raise ValueError("threshold must be between 1 and 100")
    cosine_threshold = threshold / 100.0
    # For cosine distance d = 1 - cosine, OpenSearch uses (2 - d) / 2.
    return (1.0 + cosine_threshold) / 2.0


def cosine_percentage(opensearch_score: float) -> float:
    """Convert an OpenSearch cosine score into a percentage.

    Input:
        opensearch_score=0.95
    Output:
        90.0
    """

    if opensearch_score <= 0:
        raise ValueError("opensearch_score must be greater than zero")
    cosine_similarity = (2.0 * opensearch_score) - 1.0
    cosine_similarity = max(-1.0, min(1.0, cosine_similarity))
    return round(max(0.0, cosine_similarity) * 100.0, 2)


def cosine_distance(opensearch_score: float) -> float:
    """Convert an OpenSearch cosine score into cosine distance.

    Input:
        opensearch_score=0.95
    Output:
        0.10, which represents 90 percent cosine similarity.
    """

    if opensearch_score <= 0:
        raise ValueError("opensearch_score must be greater than zero")
    cosine_similarity = (2.0 * opensearch_score) - 1.0
    cosine_similarity = max(-1.0, min(1.0, cosine_similarity))
    return round(1.0 - cosine_similarity, 8)


def encode_page_token(state: dict) -> str:
    """Put the OpenSearch cursor and request state into one token."""

    payload = json.dumps(state, separators=(",", ":"), sort_keys=True)
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")


def decode_page_token(token: str) -> dict:
    """Read a pagination token and reject broken values."""

    try:
        payload = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        state = json.loads(payload)
    except (
        ValueError,
        UnicodeDecodeError,
        binascii.Error,
        json.JSONDecodeError,
    ) as error:
        raise ValueError("Invalid nextToken") from error
    if not isinstance(state, dict):
        raise ValueError("Invalid nextToken")
    has_global_id_cursor = state.get("afterGlobalId") is not None
    has_catalog_cursor = state.get("afterKey") is not None
    has_application_offset = state.get("applicationOffset") is not None
    if not (
        has_global_id_cursor
        or has_catalog_cursor
        or has_application_offset
    ):
        raise ValueError("Invalid nextToken")
    return state
