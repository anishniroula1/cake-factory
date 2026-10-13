import hashlib
import unicodedata


def normalize_sentence(value: str) -> str:
    """Clean sentence text before making its key.

    Input:  The   Government approved it.
    Output: the government approved it.
    """

    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def sentence_key(value: str) -> str:
    """Create the same SHA-256 key for the same cleaned sentence.

    Input: THE Story and the   story.
    Output: the same 64-character key for both values.
    """

    normalized = normalize_sentence(value)
    if not normalized:
        raise ValueError("sentence content cannot be empty")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
