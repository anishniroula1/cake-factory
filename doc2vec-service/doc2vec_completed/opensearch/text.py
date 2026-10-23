import hashlib
import re
import unicodedata
from itertools import groupby

# Match words made from Unicode letters. Apostrophes and hyphens are allowed
# only when they connect letters, so standalone punctuation is not a word.
WORD_PATTERN = re.compile(r"[^\W\d_]+(?:[-'’][^\W\d_]+)*", re.UNICODE)
EXCESSIVE_REPEAT_MINIMUM = 3


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
    """Count meaningful words and collapse excessive consecutive repetition.

    Input: value="  The applicant moved to New York.  "
    Output: 6

    Input: value="Yes yes yes yes yes"
    Output: 1 because a run of at least three identical words counts once.

    Input: value="NO no No, I did not go"
    Output: 5 because the three consecutive forms of "no" count once.

    Input: value="I was very very afraid"
    Output: 5 because a two-word repetition is kept.

    Input: value="The man said the man escaped"
    Output: 6 because the repeated words are not consecutive.

    Input: value="Yes, no, 36, and 2024"
    Output: 3 because punctuation and standalone numbers are not words.

    Input: value="La policía me detuvo durante tres días"
    Output: 7 because Unicode letters are preserved.
    """

    normalized = unicodedata.normalize("NFKC", value)
    words = [word.casefold() for word in WORD_PATTERN.findall(normalized)]
    adjusted_count = 0

    for _, repeated_words in groupby(words):
        repeated_count = sum(1 for _ in repeated_words)
        adjusted_count += (
            1
            if repeated_count >= EXCESSIVE_REPEAT_MINIMUM
            else repeated_count
        )

    return adjusted_count
