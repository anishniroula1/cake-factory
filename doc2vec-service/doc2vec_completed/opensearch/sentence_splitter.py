import re

from syntok.segmenter import segment
from syntok.tokenizer import Tokenizer

MASK_PATTERNS = (
    re.compile(r"(?<=\s)([0-9]{1,2}\.)(?=\s+[1A-Z][\D])", re.ASCII),
    re.compile(r"(?<=[A-Za-z])([\.\?!]+\s+[0-9]{3})(?=[0-9,]*[\s])", re.ASCII),
    re.compile(r"(?<=\s)(i)(?=\s)", re.ASCII),
    re.compile(r"(?<=\s)(ago|may|set|art|cap)(?=\.)", re.ASCII),
)


def split_document(content: str) -> list[str]:
    """Split document text while returning the original unmodified sentences.

    Input: content="I arrived yesterday. I stayed with my aunt."
    Output: ["I arrived yesterday.", "I stayed with my aunt."]

    A masked copy is sent to Syntok so known numbering and abbreviation cases
    split correctly. The returned text is sliced from the original document,
    so temporary mask characters never enter PostgreSQL or OpenSearch.
    """

    if not content or re.fullmatch(r"[\s¶]*", content):
        return []

    prepared = _prepare_for_syntok(content)
    tokenizer = Tokenizer(
        emit_hyphen_or_underscore_sep=True,
        replace_not_contraction=False,
    )
    starts = [
        sentence[0].offset
        for sentence in segment(tokenizer.tokenize(prepared))
        if sentence
    ]
    if not starts:
        return []

    stops = [*starts[1:], len(content)]
    return [
        sentence
        for start, stop in zip(starts, stops, strict=True)
        if (sentence := content[start:stop].strip())
    ]


def _prepare_for_syntok(content: str) -> str:
    """Mask known Syntok edge cases without changing text positions."""

    prepared = content
    for pattern in MASK_PATTERNS:
        matches = list(pattern.finditer(prepared))
        for match in reversed(matches):
            prepared = prepared[: match.end() - 1] + "X" + prepared[match.end() :]
    return prepared
