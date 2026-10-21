import pytest

from doc2vec_completed.opensearch.text import (
    normalize_sentence,
    sentence_key,
    sentence_word_count,
)


def test_sentence_normalization_and_key():
    assert normalize_sentence("  THE   Story  ") == "the story"
    assert sentence_key("THE Story") == sentence_key(" the   story ")
    assert sentence_word_count("  The applicant moved to New York.  ") == 6


def test_empty_sentence_key_is_rejected():
    with pytest.raises(ValueError):
        sentence_key("   ")
