import pytest

from doc2vec_service.text import normalizeSentence, sentenceKey, splitSentences


def test_sentence_normalization_key_and_splitter():
    assert normalizeSentence("  THE   Story  ") == "the story"
    assert sentenceKey("THE Story") == sentenceKey(" the   story ")
    result = splitSentences("First sentence. Second sentence!\nThird?")
    assert [item.sentIdLocal for item in result] == [1, 2, 3]


def test_empty_sentence_key_is_rejected():
    with pytest.raises(ValueError):
        sentenceKey("   ")

