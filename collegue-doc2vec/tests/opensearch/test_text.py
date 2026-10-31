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
    assert sentence_word_count("Yes , no - 36 !!!") == 2
    assert sentence_word_count("A well-known person's story") == 4
    assert sentence_word_count(" , - !!! 36 ") == 0


def test_empty_sentence_key_is_rejected():
    with pytest.raises(ValueError):
        sentence_key("   ")


def test_word_count_collapses_only_excessive_consecutive_repetition():
    assert sentence_word_count("Yes yes yes yes yes") == 1
    assert sentence_word_count("NO no No, I did not go") == 5
    assert sentence_word_count("I was very very afraid") == 5
    assert sentence_word_count("The man said the man escaped") == 6
    assert sentence_word_count("La policía me detuvo durante tres días") == 7

