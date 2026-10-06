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


def generateFakeListRequest(request):
    """Generate production-style global IDs and a JSONB match list."""

    randomGenerator = random.Random(request.randomSeed)
    matches = []

    for number in range(request.matchCount):
        globalId = number + 10
        matches.append(
            {
                "global_id": globalId,
                # Keep one exact result so the generated verbatim array is
                # visible immediately when this fake request is inserted.
                "distance": (
                    0.0
                    if number == 0
                    else round(randomGenerator.uniform(0.01, 0.30), 4)
                ),
                "tsp_id": 1_000 + globalId,
                "a_number": 90_000 + globalId,
            }
        )

    return {
        "sourceGlobalId": 1,
        "sourceTspId": 1_001,
        "sourceANumber": 90_001,
        "matches": matches,
    }
