import hashlib
import re
import unicodedata

# Match words made from Unicode letters. Apostrophes and hyphens are allowed
# only when they connect letters, so standalone punctuation is not a word.
WORD_PATTERN = re.compile(r"[^\W\d_]+(?:[-'’][^\W\d_]+)*", re.UNICODE)


def normalize_sentence(value: str) -> str:
    """Normalize sentence text before creating its stable key.

    Input: value="  The   Government approved it.  "
    Output: "the government approved it."
    """

    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def sentence_key(value: str) -> str:
    """Create a stable SHA-256 key from normalized sentence text.

    Input: value="THE Story" or value="the   story"
    Output: the same 64-character lowercase SHA-256 key for both values.
    """

    normalized = normalize_sentence(value)
    if not normalized:
        raise ValueError("sentence content cannot be empty")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def sentence_word_count(value: str) -> int:
    """Count words directly from sentence text instead of using the saved
    sentences_vector.word_count value.

    Input: value="  The applicant moved to New York.  "
    Output: 6

    Input: value="Yes , no - 36 !!!"
    Output: 2 because punctuation and numbers are not words.
    """

    normalized = unicodedata.normalize("NFKC", value)
    return len(WORD_PATTERN.findall(normalized))
