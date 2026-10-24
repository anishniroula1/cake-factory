import re

from syntok.segmenter import segment
from syntok.tokenizer import Tokenizer

# Syntok sometimes treats numbers, lowercase "i", or uncommon abbreviations
# as the wrong sentence boundary. These patterns find those special cases in a
# temporary copy of the document. The last matched character is replaced with
# "X" because Syntok recognizes an uppercase letter more reliably.
#
# The replacement keeps the text at the same length. Syntok therefore returns
# offsets that can still be used to slice the original, unchanged document.
MASK_PATTERNS = (
    # Keep a numbered item together. Example: "42. The day was cold" becomes
    # "42X The day was cold" while Syntok calculates sentence boundaries.
    re.compile(r"(?<=\s)([0-9]{1,2}\.)(?=\s+[1A-Z][\D])", re.ASCII),
    # Help Syntok split before a year or another long number. Example:
    # "It ended. 1987 was different" receives an uppercase marker in "1987".
    re.compile(r"(?<=[A-Za-z])([\.\?!]+\s+[0-9]{3})(?=[0-9,]*[\s])", re.ASCII),
    # Help Syntok recognize a sentence that starts with lowercase "i".
    # Example: "It ended. i went home" is checked as "It ended. X went home".
    re.compile(r"(?<=\s)(i)(?=\s)", re.ASCII),
    # Stop uncommon abbreviation rules from hiding a real sentence ending.
    # Example: "It happened days ago. Then I left" masks the "o" in "ago".
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
    """Mask known Syntok edge cases without changing character positions.

    Input: content="It happened days ago. Then I left."
    Output: "It happened days agX. Then I left."

    This output is used only to calculate sentence boundaries. split_document
    applies those boundaries to the original content, so PostgreSQL and
    OpenSearch receive "ago", not "agX".
    """

    prepared = content
    for pattern in MASK_PATTERNS:
        matches = list(pattern.finditer(prepared))
        for match in reversed(matches):
            prepared = prepared[: match.end() - 1] + "X" + prepared[match.end() :]
    return prepared
