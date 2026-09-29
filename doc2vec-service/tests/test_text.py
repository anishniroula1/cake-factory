import pytest

from doc2vec_service.text import normalizeSentence, sentenceKey


def test_sentence_normalization_and_key():
    assert normalizeSentence("  THE   Story  ") == "the story"
    assert sentenceKey("THE Story") == sentenceKey(" the   story ")


def test_empty_sentence_key_is_rejected():
    with pytest.raises(ValueError):
        sentenceKey("   ")
