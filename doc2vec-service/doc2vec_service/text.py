import hashlib
import re
import unicodedata
from dataclasses import dataclass


@dataclass(frozen=True)
class SplitSentence:
    sentIdLocal: int
    sentenceContent: str
    sectionName: str


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


def splitSentences(content: str) -> list:
    """Return simple sentence records for the live-ingestion seam.

    Replace the body of this method with the existing production sentence
    splitter. The fallback keeps this standalone project runnable and splits
    on sentence-ending punctuation or a line break.
    """

    values = re.split(r"(?<=[.!?])\s+|[\r\n]+", content.strip())
    sentences = []
    for value in values:
        cleaned = value.strip()
        if cleaned:
            sentences.append(
                SplitSentence(
                    sentIdLocal=len(sentences) + 1,
                    sentenceContent=cleaned,
                    sectionName="document",
                )
            )
    return sentences
