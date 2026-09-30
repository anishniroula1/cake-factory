from types import SimpleNamespace

import pytest

from doc2vec_service import main
from doc2vec_service.dependencies import Dependencies
from doc2vec_service.handlers.historical_handler import HistoricalHandler
from doc2vec_service.text import sentenceKey
from tests.fakes import OpenSearchFake, SessionFake, SessionSequence, SqsFake


class StopApplication(Exception):
    pass


class OneMessageSqs(SqsFake):
    def __init__(self, message):
        super().__init__()
        self.message = message
        self.pollCount = 0

    def listen_sqs_message(self):
        self.pollCount += 1
        if self.pollCount == 1:
            return [self.message]
        raise StopApplication


class FlowDependencies(Dependencies):
    def __init__(self, awsSqs, historicalHandler, logger):
        self.awsSqs = awsSqs
        self.historicalHandler = historicalHandler
        self.logger = logger
        self.closed = False

    def close(self):
        self.closed = True


def historicalMessage(tsp_id=10):
    return {
        "reason": "HISTORICAL",
        "tsp_id": tsp_id,
        "MessageId": "message-1",
        "ReceiptHandle": "receipt-1",
        "message_receive_count": 1,
    }


def sentenceFrom(source, global_id, content):
    values = vars(source).copy()
    values["global_id"] = global_id
    values["sent_id_local"] = global_id
    values["sentence_content"] = content
    return SimpleNamespace(**values)


def runApplication(config, sessions, opensearch, message, logger):
    sqs = OneMessageSqs(message)
    handler = HistoricalHandler(
        config,
        sessions,
        opensearch,
        sqs,
        logger,
    )
    dependencies = FlowDependencies(sqs, handler, logger)

    with pytest.raises(StopApplication):
        main.run(dependencies=dependencies)

    assert dependencies.closed
    return sqs


def test_historical_message_runs_the_complete_ingestion_flow(
    serviceConfig,
    sentenceRecord,
    customLogger,
):
    savedVectorSentence = sentenceFrom(
        sentenceRecord,
        1,
        "The applicant moved to Boston.",
    )
    bedrockSentence = sentenceFrom(
        sentenceRecord,
        2,
        "The applicant later moved to New York.",
    )
    duplicateSentence = sentenceFrom(
        sentenceRecord,
        3,
        savedVectorSentence.sentence_content,
    )
    existingSentence = sentenceFrom(
        sentenceRecord,
        4,
        "The applicant returned home.",
    )
    rows = [
        savedVectorSentence,
        bedrockSentence,
        duplicateSentence,
        existingSentence,
    ]
    embedding = SimpleNamespace(global_id=1, embedding=[0.1, 0.2])
    saveSession = SessionFake()
    sessions = SessionSequence(
        [
            SessionFake(scalarValue=len(rows)),
            SessionFake(rows=rows),
            saveSession,
            SessionFake(rows=[embedding]),
            SessionFake(rows=[]),
        ]
    )
    opensearch = OpenSearchFake(
        existing={sentenceKey(existingSentence.sentence_content)}
    )
    message = historicalMessage()
    sqs = runApplication(
        serviceConfig,
        sessions,
        opensearch,
        message,
        customLogger,
    )

    assert saveSession.commits == 1
    assert all(len(row.sentence_key) == 64 for row in rows)
    assert opensearch.bulkCalls[0][1][0]["sentenceContentVector"] == [0.1, 0.2]
    assert opensearch.bulkCalls[1][1][0]["_id"] == bedrockSentence.sentence_key
    assert len(opensearch.bulkCalls[2][1]) == 4
    assert sqs.sent[0]["message_body"] == {
        "reason": "HISTORICAL",
        "tsp_id": 10,
    }
    assert sqs.deleted == [message]


def test_temporary_worker_failure_retries_without_losing_the_message(
    serviceConfig,
    sentenceRecord,
    customLogger,
    monkeypatch,
):
    class TemporaryFailureOpenSearch(OpenSearchFake):
        def __init__(self):
            super().__init__()
            self.readAttempts = 0

        def existingIds(self, alias, documentIds):
            self.readAttempts += 1
            if self.readAttempts == 1:
                raise RuntimeError("temporary OpenSearch failure")
            return super().existingIds(alias, documentIds)

    monkeypatch.setattr("doc2vec_service.retry.time.sleep", lambda value: None)
    rows = [sentenceRecord]
    sessions = SessionSequence(
        [
            SessionFake(scalarValue=1),
            SessionFake(rows=rows),
            SessionFake(),
            SessionFake(rows=[]),
            SessionFake(rows=[]),
            SessionFake(rows=[]),
        ]
    )
    opensearch = TemporaryFailureOpenSearch()
    message = historicalMessage()
    sqs = runApplication(
        serviceConfig,
        sessions,
        opensearch,
        message,
        customLogger,
    )

    assert opensearch.readAttempts == 2
    assert sqs.deleted == [message]
    assert len(customLogger.warningMessages) == 1


def test_failed_historical_message_is_not_deleted(
    serviceConfig,
    customLogger,
):
    sqs = runApplication(
        serviceConfig,
        SessionSequence([SessionFake(scalarValue=0)]),
        OpenSearchFake(),
        historicalMessage(404),
        customLogger,
    )

    assert sqs.deleted == []
    assert sqs.sent == []
    assert len(customLogger.exceptionMessages) == 1


@pytest.mark.parametrize("reason", [None, "TRAIN", "TRACER"])
def test_only_historical_messages_enter_the_handler(
    reason,
    serviceConfig,
    customLogger,
):
    message = historicalMessage()
    if reason is None:
        message.pop("reason")
    else:
        message["reason"] = reason

    sqs = runApplication(
        serviceConfig,
        SessionSequence([]),
        OpenSearchFake(),
        message,
        customLogger,
    )

    assert sqs.deleted == []
    assert sqs.sent == []
    assert len(customLogger.warningMessages) == 1
