import json

import pytest

from sentence_similarity.errors import PermanentDataError
from sentence_similarity.handlers.sentence_similarity_handler import (
    SentenceSimilarityHandler,
)
from tests.fakes import (
    OpenSearchHandlerFake,
    SessionFake,
    SessionSequence,
    SqsFake,
)


def test_handler_processes_page_saves_matches_and_publishes(
    serviceConfig,
    sentenceRow,
):
    classifySession = SessionFake()
    matchSession = SessionFake(executeValue=1)
    stateSession = SessionFake()
    sessions = SessionSequence(
        [
            SessionFake(scalarValue=1),
            SessionFake(rows=[sentenceRow]),
            classifySession,
            SessionFake(rows=["b" * 64]),
            matchSession,
            SessionFake(rows=[]),
            stateSession,
        ]
    )
    opensearch = OpenSearchHandlerFake(sentenceRow.sentence_key)
    sqs = SqsFake()
    handler = SentenceSimilarityHandler(serviceConfig, sessions, opensearch, sqs)

    result = handler.process(10)

    assert result == {
        "tspId": 10,
        "sourceCount": 1,
        "processedCount": 1,
        "nonMatcherCount": 0,
        "savedRelationshipCount": 1,
    }
    assert classifySession.commits == 1
    assert matchSession.commits == 1
    assert stateSession.commits == 1
    assert opensearch.updated[0][0] == 101
    assert json.loads(sqs.sent[0]["MessageBody"]) == {"tsp_id": 10}
    assert sqs.sent[0]["MessageGroupId"] == "10"


def test_non_matcher_updates_flags_and_skips_catalog(serviceConfig, sentenceRow):
    sessions = SessionSequence([SessionFake()])
    opensearch = OpenSearchHandlerFake(sentenceRow.sentence_key, nonMatcher=True)
    handler = SentenceSimilarityHandler(
        serviceConfig,
        sessions,
        opensearch,
        SqsFake(),
    )

    result = handler._processSentence(sentenceRow, [0.1, 0.2])

    assert result == {"isNonMatcher": True, "savedRelationships": 0}
    assert opensearch.catalogCalls == 0


def test_process_stops_when_catalog_vector_is_missing(serviceConfig, sentenceRow):
    sessions = SessionSequence(
        [
            SessionFake(scalarValue=1),
            SessionFake(rows=[sentenceRow]),
        ]
    )
    opensearch = OpenSearchHandlerFake(sentenceRow.sentence_key)
    opensearch.catalogVectors = missingVectors
    handler = SentenceSimilarityHandler(
        serviceConfig,
        sessions,
        opensearch,
        SqsFake(),
    )

    with pytest.raises(PermanentDataError):
        handler.process(10)


def missingVectors(sentenceKeys):
    return {}


def test_match_writes_are_split_into_small_commits(serviceConfig, sentenceRow):
    first = SessionFake(executeValue=2)
    second = SessionFake(executeValue=1)
    handler = SentenceSimilarityHandler(
        serviceConfig,
        SessionSequence([first, second]),
        OpenSearchHandlerFake(sentenceRow.sentence_key),
        SqsFake(),
    )
    matches = {
        "b" * 64: 0.1,
        "c" * 64: 0.2,
        "d" * 64: 0.3,
    }

    assert handler._saveMatches("Asylee", sentenceRow.sentence_key, matches) == 3
    assert first.commits == 1
    assert second.commits == 1
    assert handler._saveMatches("Asylee", sentenceRow.sentence_key, {}) == 0


def test_non_fifo_publish_has_no_fifo_fields(serviceConfig, sentenceRow):
    serviceConfig.nextQueueUrl = "https://sqs.example.com/standard"
    sqs = SqsFake()
    handler = SentenceSimilarityHandler(
        serviceConfig,
        SessionSequence([]),
        OpenSearchHandlerFake(sentenceRow.sentence_key),
        sqs,
    )

    handler._publish(10)

    assert "MessageGroupId" not in sqs.sent[0]
