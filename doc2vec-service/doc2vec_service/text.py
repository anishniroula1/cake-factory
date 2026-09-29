import hashlib
import unicodedata


def normalizeSentence(value: str) -> str:
    """Normalize sentence text before creating its stable key.

    Input:  The   Government approved it.
    Output: the government approved it.
    """

    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def sentenceKey(value: str) -> str:
    """Create the same SHA-256 key whenever normalized text is the same."""

    normalized = normalizeSentence(value)
    if not normalized:
        raise ValueError("sentence content cannot be empty")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
