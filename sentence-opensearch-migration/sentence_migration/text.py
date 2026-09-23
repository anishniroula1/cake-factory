import hashlib
import unicodedata


def normalize_sentence(value: str) -> str:
    """Normalize exactly like the sentence-semantic-search application."""

    normalized = unicodedata.normalize("NFKC", value).casefold()
    return " ".join(normalized.split())


def sentence_key(value: str) -> str:
    """Return the stable SHA-256 key for normalized sentence text."""

    normalized = normalize_sentence(value)
    if not normalized:
        raise ValueError("sentenceContent cannot be empty")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
