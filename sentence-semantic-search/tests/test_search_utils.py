import pytest

from sentence_search.search_utils import (
    cosine_distance,
    cosine_percentage,
    decode_page_token,
    encode_page_token,
    minimum_opensearch_score,
)


def test_ninety_percent_uses_opensearch_cosine_score():
    score = minimum_opensearch_score(90)

    assert score == pytest.approx(0.95)
    assert cosine_percentage(score) == 90.0
    assert cosine_distance(score) == 0.1


def test_application_page_token_round_trip():
    state = {
        "type": "summary",
        "afterGlobalId": 22,
        "totalSentences": 120,
    }
    token = encode_page_token(state)
    assert decode_page_token(token) == state


def test_catalog_page_token_round_trip():
    state = {
        "type": "sentenceSearch",
        "afterKey": {"sentenceKey": "key-100"},
        "returnedMatches": 100,
    }
    token = encode_page_token(state)
    assert decode_page_token(token) == state


def test_invalid_threshold_is_rejected():
    with pytest.raises(ValueError):
        minimum_opensearch_score(0)
