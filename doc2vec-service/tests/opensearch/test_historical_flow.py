import pytest

from doc2vec_completed.opensearch import main
from doc2vec_completed.opensearch.dependencies import Dependencies
from doc2vec_completed.opensearch.handlers.historical_handler import HistoricalHandler
from doc2vec_completed.opensearch.text import sentence_key
from ta_db_connection.atables import Embeddings, SentencesVector
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


def run_application(
    config,
    sessions,
    opensearch,
    message,
    logger,
    expect_processing_error=False,
):
    logger.raise_fatal_errors = not expect_processing_error
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
    saved_vector_sentence.sentence_key = None
    bedrock_sentence.sentence_key = None
    duplicate_sentence.sentence_key = None
    existing_key = sentence_key(existing_sentence.sentence_content)
    existing_sentence.sentence_key = existing_key
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
    load_session = SessionFake(rows=rows)
    save_session = SessionFake()
    sessions = SessionSequence(
        [
            SessionFake(scalar_value=len(rows)),
            load_session,
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
    assert existing_sentence.sentence_key == existing_key
    page_query = str(load_session.scalars_statements[0])
    assert "embedding_list" not in page_query
    saved_key_updates = save_session.executed[0][1]
    assert len(saved_key_updates) == 3
    calls_with_documents = [call for call in opensearch.bulk_calls if call[1]]
    saved_vector_call = calls_with_documents[0]
    occurrence_call = calls_with_documents[2]

    assert [(call[0], call[2]) for call in calls_with_documents] == [
        ("catalog-asylee", False),
        ("catalog-asylee", True),
        ("occurrence", False),
    ]
    assert saved_vector_call[1][0]["sentenceContentVector"] == [0.1, 0.2]
    assert saved_vector_call[1][0]["wordCount"] == 5
    assert len(occurrence_call[1]) == 4
    occurrence = occurrence_call[1][0]
    assert occurrence["globalId"] == 1
    assert occurrence["applicationId"] == 1001
    assert occurrence["analysisGroup"] == "Asylee"
    assert opensearch.refresh_alias_calls == [["catalog-asylee", "occurrence"]]
    assert sqs.sent[0]["message_body"] == {
        "reason": "HISTORICAL",
        "tsp_id": 10,
    }
    assert sqs.deleted == [message]


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

        def find_existing_document_ids(self, alias, document_ids):
            self.read_attempts += 1
            if self.read_attempts == 1:
                raise ConnectionError("temporary OpenSearch failure")
            return super().find_existing_document_ids(alias, document_ids)

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
    assert "temporary OpenSearch failure" not in str(custom_logger.warning_messages)


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

    assert {call[0] for call in opensearch.existing_id_calls} == {"catalog-refugee"}
    occurrence_calls = [
        call for call in opensearch.bulk_calls if call[0] == "occurrence"
    ]
    assert occurrence_calls[0][1][0]["analysisGroup"] == "Refugee"
    assert opensearch.refresh_alias_calls == [["catalog-refugee", "occurrence"]]
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
        expect_processing_error=True,
    )

    assert sqs.deleted == []
    assert sqs.sent == []


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
        expect_processing_error=True,
    )

    assert sqs.deleted == []
    assert sqs.sent == []


@pytest.mark.parametrize("reason", ["TRAIN", "UNKNOWN"])
def test_only_historical_messages_enter_the_handler(
    reason,
    service_config,
    custom_logger,
):
    message = historical_message()
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
