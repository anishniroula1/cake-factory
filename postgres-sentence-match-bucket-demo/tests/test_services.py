from types import SimpleNamespace

import pytest

from bucket_demo.init_service import generateFakeRequest
from bucket_demo.init_service import sentenceKey
from bucket_demo.python_service import buildPythonBucketRows
from bucket_demo.python_service import pythonMatchBucket
from bucket_demo.python_service import validatePythonRequest


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


def matchRequest(fakeRequest):
    """Convert a generated dictionary into a request-like object."""

    return SimpleNamespace(**fakeRequest)


def testSentenceKeyNormalizesCaseAndSpaces():
    firstKey = sentenceKey("  The Applicant   Told The Story. ")
    secondKey = sentenceKey("the applicant told the story.")

    assert firstKey == secondKey
    assert len(firstKey) == 64


def testPythonAndPostgresBucketCalculationsUseTheFirstByte():
    assert pythonMatchBucket("01" + ("0" * 62)) == 1
    assert pythonMatchBucket("20" + ("0" * 62)) == 0
    assert pythonMatchBucket("ab" + ("0" * 62)) == 11
    assert pythonMatchBucket("ff" + ("0" * 62)) == 31


def testFakeRequestContainsTheRequestedNumberOfMatches():
    fakeRequest = generateFakeRequest(initRequest(matchCount=7))

    assert fakeRequest["analysisGroup"] == "Asylee"
    assert len(fakeRequest["sourceSentenceKey"]) == 64
    assert len(fakeRequest["matches"]) == 7


def testPythonRowsContainForwardAndReverseDirections():
    fakeRequest = generateFakeRequest(initRequest(matchCount=5))
    request = matchRequest(fakeRequest)
    rows = buildPythonBucketRows(request)

    forwardMatches = {}
    reverseCount = 0

    for row in rows:
        if row["sentenceKey"] == request.sourceSentenceKey:
            forwardMatches.update(row["incomingMatches"])
        elif request.sourceSentenceKey in row["incomingMatches"]:
            reverseCount += 1

    assert forwardMatches == request.matches
    assert reverseCount == len(request.matches)


def testBadDistanceIsRejected():
    fakeRequest = generateFakeRequest(initRequest())
    request = matchRequest(fakeRequest)
    firstKey = next(iter(request.matches))
    request.matches[firstKey] = 3

    with pytest.raises(ValueError, match="between 0 and 2"):
        validatePythonRequest(request)
