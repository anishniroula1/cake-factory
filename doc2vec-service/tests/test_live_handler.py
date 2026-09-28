import json
from types import SimpleNamespace

import pytest

from doc2vec_service.errors import PermanentDataError
from doc2vec_service.handlers.live_handler import LiveHandler
from tests.fakes import OpenSearchFake, SessionFake, SessionSequence, SqsFake


def sourceDocument():
    return SimpleNamespace(
        tsp_id=10,
        application_id="APP-1",
        a_number="A-1",
        content="First sentence. Second sentence.",
        source_type="AFFIDAVIT",
        analysis_group="Asylee",
    )


def test_live_handler_runs_the_complete_flow(serviceConfig, sentenceRecord):
    saveSession = SessionFake()
    updateSession = SessionFake()
    sessions = SessionSequence(
        [
            SessionFake(document=sourceDocument()),
            saveSession,
            SessionFake(rows=[sentenceRecord]),
            SessionFake(rows=[]),
            SessionFake(rows=[]),
            updateSession,
        ]
    )
    opensearch = OpenSearchFake()
    sqs = SqsFake()
    handler = LiveHandler(serviceConfig, sessions, opensearch, sqs)

    result = handler.process(10)

    assert result["sourceCount"] == 2
    assert result["indexedCount"] == 1
    assert saveSession.commits == 1
    assert updateSession.commits == 1
    assert len(sqs.sent) == 1
    assert json.loads(sqs.sent[0]["MessageBody"])["tsp_id"] == 10
    assert [call[0] for call in opensearch.bulkCalls] == [
        "catalog",
        "catalog",
        "occurrence",
    ]


def test_live_handler_reuses_saved_vector(serviceConfig, sentenceRecord):
    embedding = SimpleNamespace(global_id=1, embedding=[0.1, 0.2])
    sessions = SessionSequence([SessionFake(rows=[embedding])])
    opensearch = OpenSearchFake()
    handler = LiveHandler(serviceConfig, sessions, opensearch, SqsFake())

    assert handler._processWorker([sentenceRecord]) == 1
    catalogDocument = opensearch.bulkCalls[0][1][0]
    assert catalogDocument["sentenceContentVector"] == [0.1, 0.2]


def test_live_handler_rejects_missing_or_empty_document(serviceConfig):
    missing = LiveHandler(
        serviceConfig,
        SessionSequence([SessionFake(document=None)]),
        OpenSearchFake(),
        SqsFake(),
    )
    with pytest.raises(PermanentDataError):
        missing.process(10)

    empty = LiveHandler(
        serviceConfig,
        SessionSequence([SessionFake(document=SimpleNamespace(content="   "))]),
        OpenSearchFake(),
        SqsFake(),
    )
    with pytest.raises(PermanentDataError):
        empty.process(10)


def test_live_handler_can_run_without_next_queue(serviceConfig):
    serviceConfig.nextQueueUrl = ""
    sqs = SqsFake()
    handler = LiveHandler(serviceConfig, SessionSequence([]), OpenSearchFake(), sqs)
    handler._publish(10)
    assert sqs.sent == []
