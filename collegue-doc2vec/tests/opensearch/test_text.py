import pytest

from doc2vec_completed.opensearch.text import (
    normalize_sentence,
    sentence_key,
    sentence_word_count,
)


def test_sentence_normalization_and_key():
    assert (
        normalize_sentence("  THE   Story  "),
        sentence_key("THE Story") == sentence_key(" the   story "),
    ) == ("the story", True)


def test_empty_sentence_key_is_rejected():
    with pytest.raises(ValueError):
        sentence_key("   ")


@pytest.mark.parametrize(
    ("sentence", "expected_count"),
    [
        ("The applicant moved to New York.", 6),
        ("Yes , no - 36 !!!", 2),
        ("A well-known person's story", 4),
        (" , - !!! 36 ", 0),
        ("Yes yes yes yes yes", 1),
        ("NO no No, I did not go", 5),
        ("I was very very afraid", 5),
        ("The man said the man escaped", 6),
        ("La policía me detuvo durante tres días", 7),
    ],
)
def test_sentence_word_count(sentence, expected_count):
    assert sentence_word_count(sentence) == expected_count

