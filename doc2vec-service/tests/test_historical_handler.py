from types import SimpleNamespace

import pytest

from doc2vec_service.errors import PermanentDataError
from doc2vec_service.handlers.historical_handler import HistoricalHandler
from tests.fakes import OpenSearchFake, SessionFake, SessionSequence, SqsFake


def test_historical_handler_indexes_saved_sentences_and_publishes(
    serviceConfig,
    sentenceRecord,
    customLogger,
):
    embedding = SimpleNamespace(global_id=1, embedding=[0.1, 0.2])
    saveSession = SessionFake()
    sessions = SessionSequence(
        [
            SessionFake(scalarValue=1),
            SessionFake(rows=[sentenceRecord]),
            saveSession,
            SessionFake(rows=[embedding]),
            SessionFake(rows=[]),
        ]
    )
    opensearch = OpenSearchFake()
    sqs = SqsFake()
    handler = HistoricalHandler(
        serviceConfig,
        sessions,
        opensearch,
        sqs,
        customLogger,
    )

    result = handler.process(10)

    assert result == {
        "reason": "HISTORICAL",
        "tsp_id": 10,
        "sourceCount": 1,
        "indexedCount": 1,
    }
    assert saveSession.commits == 1
    assert len(sentenceRecord.sentence_key) == 64
    assert [call[0] for call in opensearch.bulkCalls] == [
        "catalog",
        "catalog",
        "occurrence",
    ]
    assert opensearch.bulkCalls[0][1][0]["sentenceContentVector"] == [0.1, 0.2]
    assert sqs.sent[0]["message_body"] == {
        "tsp_id": 10,
        "reason": "HISTORICAL",
    }


def test_historical_handler_uses_bedrock_for_a_missing_vector(
    serviceConfig,
    sentenceRecord,
    customLogger,
):
    handler = HistoricalHandler(
        serviceConfig,
        SessionSequence([SessionFake(scalarValue=0)]),
        OpenSearchFake(),
        SqsFake(),
        customLogger,
    )

    assert handler._processWorker([sentenceRecord]) == 1
    catalogCall = handler.opensearch.bulkCalls[1]
    assert catalogCall[0] == "catalog"
    assert catalogCall[2] is True
    assert "sentenceContentVector" not in catalogCall[1][0]


def test_historical_handler_skips_an_existing_catalog_key_but_saves_occurrence(
    serviceConfig,
    sentenceRecord,
    customLogger,
):
    opensearch = OpenSearchFake(existing={sentenceRecord.sentence_key})
    handler = HistoricalHandler(
        serviceConfig,
        SessionSequence([SessionFake(rows=[])]),
        opensearch,
        SqsFake(),
        customLogger,
    )

    assert handler._processWorker([sentenceRecord]) == 1
    assert opensearch.bulkCalls[0][1] == []
    assert opensearch.bulkCalls[1][1] == []
    assert opensearch.bulkCalls[2][0] == "occurrence"


def test_historical_handler_rejects_a_document_without_sentences(
    serviceConfig,
    customLogger,
):
    handler = HistoricalHandler(
        serviceConfig,
        SessionSequence([SessionFake(rows=[])]),
        OpenSearchFake(),
        SqsFake(),
        customLogger,
    )

    with pytest.raises(PermanentDataError):
        handler.process(404)


def test_historical_handler_can_finish_without_a_next_queue(
    serviceConfig,
    customLogger,
):
    serviceConfig.nextQueueUrl = ""
    sqs = SqsFake()
    handler = HistoricalHandler(
        serviceConfig,
        SessionSequence([]),
        OpenSearchFake(),
        sqs,
        customLogger,
    )

    handler._publish(10)
    assert sqs.sent == []


def test_historical_handler_raises_when_the_next_message_cannot_be_sent(
    serviceConfig,
    customLogger,
):
    sqs = SqsFake()
    sqs.sendResult = False
    handler = HistoricalHandler(
        serviceConfig,
        SessionSequence([]),
        OpenSearchFake(),
        sqs,
        customLogger,
    )

    with pytest.raises(RuntimeError):
        handler._publish(10)
