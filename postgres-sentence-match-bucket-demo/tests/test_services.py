from types import SimpleNamespace

from bucket_demo.init_service import (
    generateFakeListRequest,
    generateFakeRequest,
    sentenceKey,
)


def initRequest(**changes):
    """Create a small fake init request."""

    values = {
        "analysisGroup": "Asylee",
        "matchCount": 5,
        "randomSeed": 42,
        "clearExisting": True,
    }
    values.update(changes)

    return SimpleNamespace(**values)


def testSentenceKeyNormalizesCaseAndSpaces():
    firstKey = sentenceKey("  The Applicant   Told The Story. ")
    secondKey = sentenceKey("the applicant told the story.")

    assert firstKey == secondKey
    assert len(firstKey) == 64


def testFakeRequestContainsTheRequestedNumberOfMatches():
    fakeRequest = generateFakeRequest(initRequest(matchCount=7))

    assert fakeRequest["analysisGroup"] == "Asylee"
    assert len(fakeRequest["sourceSentenceKey"]) == 64
    assert len(fakeRequest["matches"]) == 7


def testFakeListRequestUsesProductionJsonShape():
    fakeRequest = generateFakeListRequest(initRequest(matchCount=3))

    assert fakeRequest["sourceGlobalId"] == 1
    assert len(fakeRequest["matches"]) == 3
    assert set(fakeRequest["matches"][0]) == {
        "global_id",
        "distance",
        "tsp_id",
        "a_number",
    }
    assert fakeRequest["sourceANumber"] == 90_001
    assert fakeRequest["matches"][0]["distance"] == 0.0
