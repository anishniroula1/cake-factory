import json
from unittest.mock import Mock

import pytest

from doc2vec_completed.opensearch import main
from doc2vec_completed.opensearch.dependencies import Dependencies
from doc2vec_completed.opensearch.errors import PermanentDataError, QueueSendError
from doc2vec_completed.opensearch.handlers.historical_handler import HistoricalHandler
from doc2vec_completed.opensearch.sqs_processor import SqsProcessor
from doc2vec_completed.opensearch.text import sentence_key
from ta_db_connection.atatables import Embeddings, SentencesVector
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
        "tsp_id": json.dumps([tsp_id]),
        "MessageId": "message-1",
        "ReceiptHandle": "receipt-1",
        "message_receive_count": 1,
    }


def test_historical_tsp_id_accepts_one_string_or_a_list():
    assert (
        SqsProcessor._read_tsp_ids({"tsp_id": "10"}),
        SqsProcessor._read_tsp_ids({"tsp_id": "[10, 20]"}),
        SqsProcessor._read_tsp_ids({"tsp_id": [10, 20]}),
    ) == (["10"], [10, 20], [10, 20])


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
    invalid_embedding = Embeddings(
        global_id=2,
        embedding_type=2,
        embeddings=[0.1],
    )
    load_session = SessionFake(rows=rows)
    save_session = SessionFake()
    sessions = SessionSequence(
        [
            SessionFake(scalar_value=len(rows)),
            load_session,
            save_session,
            SessionFake(rows=[embedding, invalid_embedding]),
            SessionFake(rows=[]),
        ]
    )
    opensearch = OpenSearchFake(
        existing={sentence_key(existing_sentence.sentence_content)}
    )
    message = historical_message()
    run_application(
        service_config,
        sessions,
        opensearch,
        message,
        custom_logger,
    )

    calls_with_documents = [call for call in opensearch.bulk_calls if call[1]]
    assert [(call[0], call[2]) for call in calls_with_documents] == [
        ("catalog-asylee", False),
        ("catalog-asylee", True),
        ("occurrence", False),
    ]


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
    run_application(
        service_config,
        sessions,
        opensearch,
        message,
        custom_logger,
    )

    assert opensearch.read_attempts == 2


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
    run_application(
        service_config,
        sessions,
        opensearch,
        message,
        custom_logger,
    )

    assert {call[0] for call in opensearch.existing_id_calls} == {"catalog-refugee"}


def test_failed_historical_document_is_logged_and_source_message_is_deleted(
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

    assert len(sqs.deleted) == 1
    assert any('["404"]' in message for message in custom_logger.error_messages)


def test_historical_completion_requires_the_send_event_queue(
    service_config,
    custom_logger,
):
    service_config.aws_sqs_send_event_queue_endpoint = ""
    handler = HistoricalHandler(
        service_config,
        SessionSequence([]),
        OpenSearchFake(),
        SqsFake(),
        custom_logger,
    )

    with pytest.raises(QueueSendError, match="endpoint is not configured"):
        handler._send_sqs_messages([10])


def test_historical_list_refreshes_once_and_sends_one_sqs_batch(
    service_config,
    custom_logger,
):
    opensearch = OpenSearchFake()
    sqs = SqsFake()
    handler = HistoricalHandler(
        service_config,
        SessionSequence([]),
        opensearch,
        sqs,
        custom_logger,
    )
    handler._process_historical_document = Mock(
        side_effect=[
            (
                {
                    "reason": "HISTORICAL",
                    "tsp_id": 10,
                    "source_count": 2,
                    "indexed_count": 2,
                },
                "catalog-asylee",
            ),
            None,
            (
                {
                    "reason": "HISTORICAL",
                    "tsp_id": 20,
                    "source_count": 3,
                    "indexed_count": 3,
                },
                "catalog-refugee",
            ),
        ]
    )

    result = handler.process([10, 15, 20])

    assert opensearch.refresh_alias_calls == [
        ["catalog-asylee", "catalog-refugee", "occurrence"]
    ]
    assert result["tsp_ids"] == [10, 20] and len(sqs.sent[0]["batch"]) == 2
    assert any('["15"]' in message for message in custom_logger.error_messages)


def test_historical_send_uses_ten_string_messages_per_batch(
    service_config,
    custom_logger,
):
    sqs = SqsFake()
    handler = HistoricalHandler(
        service_config,
        SessionSequence([]),
        OpenSearchFake(),
        sqs,
        custom_logger,
    )

    handler._send_sqs_messages(list(range(1, 13)))

    assert [len(call["batch"]) for call in sqs.sent] == [10, 2]
    assert all(call["retries"] == 3 for call in sqs.sent)
    assert all(call["message_group_id"] is None for call in sqs.sent)
    assert json.loads(sqs.sent[0]["batch"][0]) == {
        "source": "P2",
        "tsp_id": "1",
        "sent_by": "doc2vec",
    }


def test_failed_worker_logs_its_global_id_range(
    service_config,
    sentence_record,
    custom_logger,
):
    handler = HistoricalHandler(
        service_config,
        SessionSequence([]),
        OpenSearchFake(),
        SqsFake(),
        custom_logger,
    )
    handler.batch_processor.save_historical_sentence_batch = Mock(
        side_effect=PermanentDataError("invalid sentence batch")
    )

    with pytest.raises(PermanentDataError):
        handler._save_historical_sentence_batch_in_opensearch([sentence_record])


def test_unsupported_analysis_group_is_logged_for_manual_reprocessing(
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

    assert len(sqs.deleted) == 1
    assert any('["10"]' in message for message in custom_logger.error_messages)

