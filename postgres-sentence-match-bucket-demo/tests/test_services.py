from types import SimpleNamespace

import pytest

from bucket_demo.init_service import generateFakeRequest
from bucket_demo.init_service import generateFakeListRequest
from bucket_demo.init_service import sentenceKey
from bucket_demo.list_python_service import buildListPythonOwnerRows
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


def testListPythonRowsContainBothDirections():
    fakeRequest = generateFakeListRequest(initRequest(matchCount=3))
    request = matchRequest(fakeRequest)
    ownerRows = buildListPythonOwnerRows(request)

    assert len(ownerRows) == 4

    sourceRows = []
    reverseRows = []

    for row in ownerRows:
        if row["globalId"] == request.sourceGlobalId:
            sourceRows.append(row)
        else:
            reverseRows.append(row)

    assert len(sourceRows) == 1
    assert sourceRows[0]["incomingMatches"] == [
        {
            "global_id": match["global_id"],
            "distance": match["distance"],
            "tsp_id": match["tsp_id"],
            "a_number": match["a_number"],
        }
        for match in request.matches
    ]
    assert len(reverseRows) == 3

    for row in reverseRows:
        assert len(row["incomingMatches"]) == 1
        assert (
            row["incomingMatches"][0]["global_id"]
            == request.sourceGlobalId
        )
        assert (
            row["incomingMatches"][0]["a_number"]
            == request.sourceANumber
        )


def testListPythonRowsCreateSourceWhenMatchesAreEmpty():
    request = SimpleNamespace(
        sourceGlobalId=1,
        sourceTspId=1001,
        sourceANumber=90001,
        matches=[],
    )

    assert buildListPythonOwnerRows(request) == [
        {
            "globalId": 1,
            "tspId": 1001,
            "incomingMatches": [],
        }
    ]
