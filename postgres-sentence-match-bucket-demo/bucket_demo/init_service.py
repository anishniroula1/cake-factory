import hashlib
import random


def sentenceKey(value):
    """Create a stable SHA-256 key from sentence text.

    Input: value is "The applicant described the event."
    Output: a lowercase 64-character SHA-256 key.
    """

    normalizedValue = " ".join(value.lower().split())

    return hashlib.sha256(normalizedValue.encode("utf-8")).hexdigest()


def generateFakeRequest(request):
    """Generate fake keys that can be sent to either insert endpoint."""

    if not request.analysisGroup.strip():
        raise ValueError("analysisGroup is required")

    if request.matchCount < 1 or request.matchCount > 1_000:
        raise ValueError("matchCount must be between 1 and 1000")

    randomGenerator = random.Random(request.randomSeed)
    sourceKey = sentenceKey("Fake source sentence")
    matches = {}

    for number in range(1, request.matchCount + 1):
        matchingKey = sentenceKey(f"Fake matching sentence {number}")
        matches[matchingKey] = round(
            randomGenerator.uniform(0.01, 0.30),
            4,
        )

    return {
        "analysisGroup": request.analysisGroup,
        "sourceSentenceKey": sourceKey,
        "matches": matches,
    }
