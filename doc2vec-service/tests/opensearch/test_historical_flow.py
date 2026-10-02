import pytest

from doc2vec_completed.opensearch import main
from doc2vec_completed.opensearch.dependencies import Dependencies
from doc2vec_completed.opensearch.handlers.historical_handler import HistoricalHandler
from doc2vec_completed.opensearch.models import Embeddings, SentencesVector
from doc2vec_completed.opensearch.text import sentence_key
from tests.opensearch.fakes import OpenSearchFake, SessionFake, SessionSequence, SqsFake


class StopApplicationError(Exception):
    pass


class OneMessageSqs(SqsFake):
    def __init__(self, message):
        super().__init__()
        self.message = message
        self.poll_count = 0

    def listen_sqs_message(self):
        self.poll_count += 1
        if self.poll_count == 1:
            return [self.message]
        raise StopApplicationError


class FlowDependencies(Dependencies):
    def __init__(self, aws_sqs, historical_handler, logger):
        self.aws_sqs = aws_sqs
        self.historical_handler = historical_handler
        self.logger = logger
        self.closed = False

    def close(self):
        self.closed = True


def historical_message(tsp_id=10):
    return {
        "reason": "HISTORICAL",
        "tsp_id": tsp_id,
        "MessageId": "message-1",
        "ReceiptHandle": "receipt-1",
        "message_receive_count": 1,
    }


def sentence_from(source, global_id, content):
    values = {
        column.key: getattr(source, column.key)
        for column in SentencesVector.__table__.columns
    }
    values["global_id"] = global_id
    values["sent_id_local"] = global_id
    values["sentence_content"] = content
    return SentencesVector(**values, tsp=source.tsp)


def run_application(config, sessions, opensearch, message, logger):
    sqs = OneMessageSqs(message)
    handler = HistoricalHandler(
        config,
        sessions,
        opensearch,
        sqs,
        logger,
    )
    dependencies = FlowDependencies(sqs, handler, logger)

    with pytest.raises(StopApplicationError):
        main.run_opensearch_flow(dependencies=dependencies)

    assert dependencies.closed
    return sqs


def test_historical_message_runs_the_complete_ingestion_flow(
    service_config,
    sentence_record,
    custom_logger,
):
    saved_vector_sentence = sentence_from(
        sentence_record,
        1,
        "The applicant moved to Boston.",
    )
    bedrock_sentence = sentence_from(
        sentence_record,
        2,
        "The applicant later moved to New York.",
    )
    duplicate_sentence = sentence_from(
        sentence_record,
        3,
        saved_vector_sentence.sentence_content,
    )
    existing_sentence = sentence_from(
        sentence_record,
        4,
        "The applicant returned home.",
    )
    rows = [
        saved_vector_sentence,
        bedrock_sentence,
        duplicate_sentence,
        existing_sentence,
    ]
    embedding = Embeddings(
        global_id=1,
        embedding_type=2,
        embeddings=[0.1, 0.2],
    )
    save_session = SessionFake()
    sessions = SessionSequence(
        [
            SessionFake(scalar_value=len(rows)),
            SessionFake(rows=rows),
            save_session,
            SessionFake(rows=[embedding]),
            SessionFake(rows=[]),
        ]
    )
    opensearch = OpenSearchFake(
        existing={sentence_key(existing_sentence.sentence_content)}
    )
    message = historical_message()
    sqs = run_application(
        service_config,
        sessions,
        opensearch,
        message,
        custom_logger,
    )

    assert save_session.commits == 1
    assert all(len(row.sentence_key) == 64 for row in rows)
    calls_with_documents = [call for call in opensearch.bulk_calls if call[1]]
    saved_vector_call = calls_with_documents[0]
    bedrock_call = calls_with_documents[1]
    occurrence_call = calls_with_documents[2]

    assert saved_vector_call[0] == "catalog-asylee"
    assert saved_vector_call[1][0]["sentenceContentVector"] == [0.1, 0.2]
    assert saved_vector_call[2] is False
    assert bedrock_call[0] == "catalog-asylee"
    assert bedrock_call[1][0]["_id"] == bedrock_sentence.sentence_key
    assert bedrock_call[2] is True

    assert occurrence_call[0] == "occurrence"
    assert len(occurrence_call[1]) == 4
    occurrence = occurrence_call[1][0]
    assert occurrence["globalId"] == 1
    assert occurrence["applicationId"] == 1001
    assert occurrence["tspId"] == 10
    assert isinstance(occurrence["globalId"], int)
    assert isinstance(occurrence["applicationId"], int)
    assert isinstance(occurrence["tspId"], int)
    assert occurrence["sectionName"] == "affidavit"
    assert occurrence["analysisGroup"] == "Asylee"
    assert occurrence["isCurrent"] is True
    assert "aNumber" not in occurrence
    assert "isNonMatcher" not in occurrence
    assert "isTracer" not in occurrence
    assert "isFormLanguage" not in occurrence
    assert "nonMatcherChecked" not in occurrence
    assert {call[0] for call in opensearch.existing_id_calls} == {
        "catalog-asylee"
    }
    assert sqs.sent[0]["message_body"] == {
        "reason": "HISTORICAL",
        "tsp_id": 10,
    }
    assert sqs.deleted == [message]

    expected_log_order = [
        "Starting historical SQS message",
        "Historical sentence processing started",
        "Counting historical sentences",
        "Found 4 historical sentence(s)",
        "Historical page 1 loaded 4 sentence(s)",
        "Creating and saving sentence keys",
        "Saved 4 sentence key(s)",
        "Starting 1 worker batch(es)",
        "worker batch for global IDs 1 through 4 started",
        "selected catalog catalog-asylee",
        "loaded 1 saved Titan embedding(s)",
        "is writing catalog catalog-asylee",
        "completed catalog catalog-asylee",
        "is writing 4 occurrence document(s)",
        "wrote 4 occurrence document(s)",
        "worker batch for global IDs 1 through 4 completed",
        "Historical page 1 completed",
        "Historical page 2 is empty",
        "Sending the completed historical message",
        "Sent the completed historical message",
        "Historical sentence processing completed",
        "finished processing. Deleting it",
        "completed and was deleted",
    ]
    next_position = 0
    for expected_message in expected_log_order:
        next_position = next(
            index + 1
            for index in range(next_position, len(custom_logger.info_messages))
            if expected_message in custom_logger.info_messages[index]
        )
    assert all(
        "tsp_id=" not in message_text
        for message_text in custom_logger.info_messages
    )


def test_temporary_worker_failure_retries_without_losing_the_message(
    service_config,
    sentence_record,
    custom_logger,
    monkeypatch,
):
    class TemporaryFailureOpenSearch(OpenSearchFake):
        def __init__(self):
            super().__init__()
            self.read_attempts = 0

        def existing_ids(self, alias, document_ids):
            self.read_attempts += 1
            if self.read_attempts == 1:
                raise RuntimeError("temporary OpenSearch failure")
            return super().existing_ids(alias, document_ids)

    monkeypatch.setattr(
        "doc2vec_completed.opensearch.retry.time.sleep", lambda value: None
    )
    rows = [sentence_record]
    sessions = SessionSequence(
        [
            SessionFake(scalar_value=1),
            SessionFake(rows=rows),
            SessionFake(),
            SessionFake(rows=[]),
            SessionFake(rows=[]),
            SessionFake(rows=[]),
        ]
    )
    opensearch = TemporaryFailureOpenSearch()
    message = historical_message()
    sqs = run_application(
        service_config,
        sessions,
        opensearch,
        message,
        custom_logger,
    )

    assert opensearch.read_attempts == 2
    assert sqs.deleted == [message]
    assert len(custom_logger.warning_messages) == 1
    assert "attempt 1 failed" in custom_logger.warning_messages[0]
    assert any(
        "failed during catalog indexing" in message_text
        for message_text in custom_logger.error_messages
    )


def test_refugee_document_uses_only_the_refugee_catalog(
    service_config,
    sentence_record,
    custom_logger,
):
    sentence_record.tsp.analysis_group = "Refugee"
    sessions = SessionSequence(
        [
            SessionFake(scalar_value=1),
            SessionFake(rows=[sentence_record]),
            SessionFake(),
            SessionFake(rows=[]),
            SessionFake(rows=[]),
        ]
    )
    opensearch = OpenSearchFake()
    message = historical_message()
    sqs = run_application(
        service_config,
        sessions,
        opensearch,
        message,
        custom_logger,
    )

    assert {call[0] for call in opensearch.existing_id_calls} == {
        "catalog-refugee"
    }
    occurrence_calls = [
        call for call in opensearch.bulk_calls if call[0] == "occurrence"
    ]
    assert occurrence_calls[0][1][0]["analysisGroup"] == "Refugee"
    assert sqs.deleted == [message]


def test_failed_historical_message_is_not_deleted(
    service_config,
    custom_logger,
):
    sqs = run_application(
        service_config,
        SessionSequence([SessionFake(scalar_value=0)]),
        OpenSearchFake(),
        historical_message(404),
        custom_logger,
    )

    assert sqs.deleted == []
    assert sqs.sent == []
    assert len(custom_logger.exception_messages) == 1


def test_unsupported_analysis_group_does_not_delete_the_message(
    service_config,
    sentence_record,
    custom_logger,
):
    sentence_record.tsp.analysis_group = "Unknown"
    sessions = SessionSequence(
        [
            SessionFake(scalar_value=1),
            SessionFake(rows=[sentence_record]),
            SessionFake(),
            SessionFake(rows=[]),
        ]
    )
    message = historical_message()
    sqs = run_application(
        service_config,
        sessions,
        OpenSearchFake(),
        message,
        custom_logger,
    )

    assert sqs.deleted == []
    assert sqs.sent == []
    assert len(custom_logger.exception_messages) == 1
    assert any(
        "failed during catalog selection" in message_text
        for message_text in custom_logger.error_messages
    )
    assert any(
        "will not be retried" in message_text
        for message_text in custom_logger.error_messages
    )


@pytest.mark.parametrize("reason", [None, "TRAIN", "TRACER"])
def test_only_historical_messages_enter_the_handler(
    reason,
    service_config,
    custom_logger,
):
    message = historical_message()
    if reason is None:
        message.pop("reason")
    else:
        message["reason"] = reason

    sqs = run_application(
        service_config,
        SessionSequence([]),
        OpenSearchFake(),
        message,
        custom_logger,
    )

    assert sqs.deleted == []
    assert sqs.sent == []
    assert len(custom_logger.warning_messages) == 1
