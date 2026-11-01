from types import SimpleNamespace

import pytest

from doc_delete_job.opensearch.document_delete_job import DocumentDeleteJob
from doc_delete_job.opensearch.errors import OperationFailedError, PermanentDataError, QueueSendError
from doc_delete_job.opensearch.handlers.document_delete_handler import DocumentDeleteHandler
from tests.opensearch.fakes import (
    AwsSqsFake,
    CatalogStoreFake,
    EmbeddingsDBStoreFake,
    HandlerFake,
    LoggerFake,
    OccurrenceStoreFake,
    SentenceVectorDBStoreFake,
    TextSimilarityInputDBStoreFake,
    TitanMatchesDBStoreFake,
)


def build_handler(
    sentence_occurrence_open_search_store=None,
    sentence_catalog_open_search_store=None,
    sentence_vector_db_store=None,
    embeddings_db_store=None,
    titan_matches_db_store=None,
    text_similarity_input_db_store=None,
    outbound_sqs=None,
    database_delete_enabled=True,
):
    logger = LoggerFake()
    return DocumentDeleteHandler(
        sentence_occurrence_open_search_store or OccurrenceStoreFake(),
        sentence_catalog_open_search_store or CatalogStoreFake(),
        sentence_vector_db_store or SentenceVectorDBStoreFake(),
        embeddings_db_store or EmbeddingsDBStoreFake(),
        titan_matches_db_store or TitanMatchesDBStoreFake(),
        text_similarity_input_db_store or TextSimilarityInputDBStoreFake(),
        outbound_sqs or AwsSqsFake(),
        "https://sqs.example/case-similarity-local.fifo",
        logger,
        database_delete_enabled,
    )


def test_document_deletion_runs_the_complete_flow():
    events = []
    occurrence_records = [
        {"sentence_key": "key-a", "analysis_group": "Asylee"},
        {"sentence_key": "key-b", "analysis_group": "Asylee"},
    ]
    occurrence_store = OccurrenceStoreFake(occurrence_records, shared_sentence_keys={"key-b"}, events=events)
    catalog_store = CatalogStoreFake(events=events)
    sentence_vector_db_store = SentenceVectorDBStoreFake(events=events)
    embeddings_db_store = EmbeddingsDBStoreFake(events=events)
    titan_matches_db_store = TitanMatchesDBStoreFake(events=events)
    state_store = TextSimilarityInputDBStoreFake(events=events)
    outbound_sqs = AwsSqsFake(events=events)
    handler = build_handler(
        occurrence_store,
        catalog_store,
        sentence_vector_db_store,
        embeddings_db_store,
        titan_matches_db_store,
        state_store,
        outbound_sqs,
    )

    deletion_result = handler.process_document_deletion(10)

    assert deletion_result == {
        "tsp_id": 10,
        "occurrence_count": 2,
        "catalog_count": 1,
        "deleted_match_bucket_count": 3,
        "deleted_embedding_count": 2,
        "deleted_sentence_count": 2,
    }
    assert occurrence_store.deleted == [10]
    assert catalog_store.deleted == [("Asylee", ["key-a"])]
    assert events == [
        "delete catalog",
        "delete occurrences",
        "delete Titan matches",
        "delete embeddings",
        "delete sentence vectors",
        "state=DOC_DELETED",
        "send SQS",
    ]
    assert state_store.state_updates == [(10, "DOC_DELETED")]
    assert outbound_sqs.sent[0]["message_body"] == {"source": "P2", "tsp_id": "10", "sent_by": "doc-delete"}


def test_missing_occurrences_do_not_drive_opensearch_cleanup_from_database():
    catalog_store = CatalogStoreFake()
    sentence_store = SentenceVectorDBStoreFake()
    handler = build_handler(OccurrenceStoreFake(), catalog_store, sentence_store)

    deletion_result = handler.process_document_deletion("10")

    assert deletion_result["occurrence_count"] == 0
    assert catalog_store.deleted == []
    assert sentence_store.deleted == ["10"]


def test_empty_sentence_vector_context_skips_embedding_deletion():
    occurrence_records = [{"sentence_key": "key-a", "analysis_group": "Asylee"}]
    sentence_vector_store = SentenceVectorDBStoreFake()
    sentence_vector_store.context = SimpleNamespace(global_ids=[])
    embeddings_store = EmbeddingsDBStoreFake()

    build_handler(
        sentence_occurrence_open_search_store=OccurrenceStoreFake(occurrence_records),
        sentence_vector_db_store=sentence_vector_store,
        embeddings_db_store=embeddings_store,
    ).process_document_deletion(10)

    assert embeddings_store.deleted == []


def test_database_deletion_toggle_still_removes_titan_matches():
    occurrence_records = [{"sentence_key": "key-a", "analysis_group": "Asylee"}]
    sentence_store = SentenceVectorDBStoreFake()
    embeddings_store = EmbeddingsDBStoreFake()
    titan_store = TitanMatchesDBStoreFake()
    state_store = TextSimilarityInputDBStoreFake()

    deletion_result = build_handler(
        sentence_occurrence_open_search_store=OccurrenceStoreFake(occurrence_records),
        sentence_vector_db_store=sentence_store,
        embeddings_db_store=embeddings_store,
        titan_matches_db_store=titan_store,
        text_similarity_input_db_store=state_store,
        database_delete_enabled=False,
    ).process_document_deletion(10)

    assert deletion_result["deleted_match_bucket_count"] == 3
    assert deletion_result["deleted_embedding_count"] == 0
    assert deletion_result["deleted_sentence_count"] == 0
    assert (sentence_store.loaded, sentence_store.deleted, embeddings_store.deleted, titan_store.deleted) == (
        [],
        [],
        [],
        [10],
    )
    assert state_store.state_updates == []


def test_handler_reports_when_next_queue_send_fails():
    state_store = TextSimilarityInputDBStoreFake()

    with pytest.raises(QueueSendError):
        build_handler(
            text_similarity_input_db_store=state_store,
            outbound_sqs=AwsSqsFake(send_result=False),
        ).process_document_deletion(10)

    assert state_store.state_updates == [(10, "DOC_DELETED"), (10, "READY_FOR_DELETION")]


def test_missing_tsi_row_stops_before_sqs_send():
    state_store = TextSimilarityInputDBStoreFake(updated_count=0)
    outbound_sqs = AwsSqsFake()

    with pytest.raises(PermanentDataError, match="updated 0"):
        build_handler(
            text_similarity_input_db_store=state_store,
            outbound_sqs=outbound_sqs,
        ).process_document_deletion(10)

    assert outbound_sqs.sent == []


def test_job_processes_ready_documents_in_order():
    document_store = TextSimilarityInputDBStoreFake([[10, 20], [30], []])
    handler = HandlerFake()
    logger = LoggerFake()
    job = DocumentDeleteJob(document_store, handler, logger)

    processed_count = job.run()

    assert (processed_count, handler.calls, logger.generic_message) == (3, [10, 20, 30], None)
    assert document_store.calls == [(None, 100), (20, 100), (30, 100)]


@pytest.mark.parametrize("error", [OperationFailedError("failed"), PermanentDataError("bad data"), QueueSendError("queue failed")])
def test_job_stops_on_failed_document(error):
    document_store = TextSimilarityInputDBStoreFake([[10]])
    logger = LoggerFake()
    job = DocumentDeleteJob(document_store, HandlerFake(error), logger)

    with pytest.raises(type(error)):
        job.run()

    assert logger.generic_message is None
