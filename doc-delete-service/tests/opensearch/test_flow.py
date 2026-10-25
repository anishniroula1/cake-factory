from unittest.mock import Mock

import pytest

from doc_delete_completed.opensearch.contracts import DocumentDeletionContext
from doc_delete_completed.opensearch.errors import OperationFailedError, PermanentDataError, QueueSendError
from doc_delete_completed.opensearch.handlers.document_delete_handler import DocumentDeleteHandler
from doc_delete_completed.opensearch.sqs_processor import SqsProcessor
from tests.opensearch.fakes import (
    AwsSqsFake,
    CatalogStoreFake,
    DependenciesFake,
    EmbeddingsDBStoreFake,
    HandlerFake,
    LoggerFake,
    OccurrenceStoreFake,
    SentenceVectorDBStoreFake,
    TitanMatchesDBStoreFake,
)


def build_handler(
    occurrence_store=None,
    catalog_store=None,
    sentence_vector_db_store=None,
    embeddings_db_store=None,
    titan_matches_db_store=None,
    aws_sqs=None,
    database_delete_enabled=True,
):
    logger = LoggerFake()
    return DocumentDeleteHandler(
        occurrence_store or OccurrenceStoreFake(),
        catalog_store or CatalogStoreFake(),
        sentence_vector_db_store or SentenceVectorDBStoreFake(),
        embeddings_db_store or EmbeddingsDBStoreFake(),
        titan_matches_db_store or TitanMatchesDBStoreFake(),
        aws_sqs or AwsSqsFake(),
        "https://sqs.example/case-similarity-local.fifo",
        logger,
        database_delete_enabled,
    )


def test_document_deletion_runs_the_complete_flow():
    events = []
    records = [
        {"global_id": 1, "sentence_key": "key-a", "analysis_group": "Asylee"},
        {"global_id": 2, "sentence_key": "key-b", "analysis_group": "Asylee"},
    ]
    occurrence_store = OccurrenceStoreFake(records, referenced={"key-b"}, events=events)
    catalog_store = CatalogStoreFake(events=events)
    sentence_vector_db_store = SentenceVectorDBStoreFake()
    embeddings_db_store = EmbeddingsDBStoreFake()
    titan_matches_db_store = TitanMatchesDBStoreFake()
    aws_sqs = AwsSqsFake()
    handler = build_handler(
        occurrence_store,
        catalog_store,
        sentence_vector_db_store,
        embeddings_db_store,
        titan_matches_db_store,
        aws_sqs,
    )

    result = handler.process(10)

    assert result == {
        "tsp_id": 10,
        "occurrence_count": 2,
        "catalog_count": 1,
        "shared_catalog_count": 1,
        "deleted_match_bucket_count": 3,
        "updated_reverse_bucket_count": 4,
        "deleted_embedding_count": 2,
        "deleted_sentence_count": 2,
        "database_delete_enabled": True,
        "next_message_sent": True,
    }
    assert occurrence_store.deleted == [1, 2]
    assert catalog_store.deleted == [("Asylee", ["key-a"])]
    assert events == ["delete catalog", "delete occurrences"]
    assert aws_sqs.sent[0]["message_body"] == {"source": "P2", "tsp_id": "10", "sent_by": "doc-delete"}


def test_missing_occurrences_do_not_drive_opensearch_cleanup_from_database():
    catalog_store = CatalogStoreFake()
    sentence_store = SentenceVectorDBStoreFake()
    handler = build_handler(OccurrenceStoreFake(), catalog_store, sentence_store)

    result = handler.process("10")

    assert result["occurrence_count"] == 0
    assert catalog_store.deleted == []
    assert sentence_store.deleted == ["10"]


def test_occurrence_global_ids_are_used_when_sentence_vector_is_empty():
    records = [{"global_id": 101, "sentence_key": "key-a", "analysis_group": "Asylee"}]
    sentence_vector_store = SentenceVectorDBStoreFake()
    sentence_vector_store.context = DocumentDeletionContext(10, None, [], [])
    embeddings_store = EmbeddingsDBStoreFake()

    build_handler(
        occurrence_store=OccurrenceStoreFake(records),
        sentence_vector_db_store=sentence_vector_store,
        embeddings_db_store=embeddings_store,
    ).process(10)

    assert embeddings_store.deleted == [101]


def test_database_deletion_toggle_keeps_postgresql_rows():
    records = [{"global_id": 1, "sentence_key": "key-a", "analysis_group": "Asylee"}]
    sentence_store = SentenceVectorDBStoreFake()
    embeddings_store = EmbeddingsDBStoreFake()
    titan_store = TitanMatchesDBStoreFake()

    result = build_handler(
        occurrence_store=OccurrenceStoreFake(records),
        sentence_vector_db_store=sentence_store,
        embeddings_db_store=embeddings_store,
        titan_matches_db_store=titan_store,
        database_delete_enabled=False,
    ).process(10)

    assert result["database_delete_enabled"] is False
    assert (sentence_store.loaded, sentence_store.deleted, embeddings_store.deleted, titan_store.deleted) == (
        [],
        [],
        [],
        [],
    )


def test_handler_keeps_message_when_next_queue_send_fails():
    with pytest.raises(QueueSendError):
        build_handler(aws_sqs=AwsSqsFake(send_result=False)).process(10)


def test_sqs_processor_deletes_only_successful_messages():
    good = {"tsp_id": 10, "ReceiptHandle": "good-local"}
    missing = {"ReceiptHandle": "missing-local"}
    aws_sqs = AwsSqsFake()
    handler = HandlerFake()
    logger = LoggerFake()
    processor = SqsProcessor(DependenciesFake(aws_sqs, handler, logger))

    processor._process_message(missing)
    processor._process_message(good)

    assert (handler.calls, aws_sqs.deleted, logger.generic_message) == ([10], [good], "tsp_id=10")


@pytest.mark.parametrize(
    "error",
    [OperationFailedError("failed"), PermanentDataError("bad data"), QueueSendError("queue failed")],
)
def test_sqs_processor_leaves_failed_message_for_sqs(error):
    message = {"tsp_id": 10, "ReceiptHandle": "failed-local"}
    aws_sqs = AwsSqsFake()
    processor = SqsProcessor(DependenciesFake(aws_sqs, HandlerFake(error), LoggerFake()))

    processor._process_message(message)

    assert aws_sqs.deleted == []


def test_sqs_processor_run_keeps_listening():
    message = {"tsp_id": 10, "ReceiptHandle": "receipt-local"}
    aws_sqs = AwsSqsFake()
    aws_sqs.listen_sqs_message = Mock(side_effect=[[message], KeyboardInterrupt])
    processor = SqsProcessor(DependenciesFake(aws_sqs, HandlerFake(), LoggerFake()))

    with pytest.raises(KeyboardInterrupt):
        processor.run()

    assert aws_sqs.deleted == [message]
