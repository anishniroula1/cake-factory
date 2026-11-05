from similarity_completed.opensearch.handlers import sentence_similarity_handler as handler_module
from similarity_completed.opensearch.handlers.sentence_similarity_handler import SentenceSimilarityHandler
from tests.opensearch.fakes import LoggerFake, SessionSequence, SqsFake, opensearch_mock, session_mock


def source_record(sentence_key):
    return {"sentence_key": sentence_key, "vector": [0.1, 0.2]}


def test_eligible_sentence_searches_catalog_and_expands_occurrences(service_config, sentence_row):
    opensearch, router = opensearch_mock(sentence_row.sentence_key)
    handler = SentenceSimilarityHandler(
        service_config, SessionSequence([]), opensearch, SqsFake(), LoggerFake()
    )

    result = handler._process_source_sentence(
        sentence_row,
        "catalog-asylee",
        source_record(sentence_row.sentence_key),
    )

    assert len(result["match_relationships"]) == 1
    assert router.semantic_search_count == 1


def test_document_without_eligible_sentences_continues_to_next_queue(service_config):
    opensearch, _ = opensearch_mock("a" * 64)
    sqs = SqsFake()
    handler = SentenceSimilarityHandler(
        service_config,
        SessionSequence([session_mock(scalar_value=0), session_mock(rows=[])]),
        opensearch,
        sqs,
        LoggerFake(),
    )

    result = handler.process_document_sentences(10)

    assert result["processed_count"] == 0
    assert len(sqs.sent) == 1


def test_occurrence_readiness_waits_once_and_then_continues(monkeypatch, service_config):
    waits = []
    opensearch, _ = opensearch_mock("a" * 64, occurrence_counts=[0, 1])
    handler = SentenceSimilarityHandler(
        service_config, SessionSequence([]), opensearch, SqsFake(), LoggerFake()
    )
    monkeypatch.setattr(handler_module.time, "sleep", waits.append)

    assert handler._wait_for_document_occurrences(10) is True
    assert waits == [30]


def test_sentence_query_loads_classification_fields_without_filtering_rows(
    service_config, sentence_row
):
    load_session = session_mock(rows=[sentence_row])
    opensearch, _ = opensearch_mock("a" * 64)
    handler = SentenceSimilarityHandler(
        service_config,
        SessionSequence([load_session]),
        opensearch,
        SqsFake(),
        LoggerFake(),
    )

    records = handler.sentence_vector_db_store.load_document_sentence_batch(10, 0)
    sql = str(load_session.scalars.call_args.args[0])

    assert records == [sentence_row]
    assert "word_count" in sql and "is_non_matcher" in sql
from types import SimpleNamespace


def test_source_sentences_are_filtered_once_before_worker_processing(service_config):
    logger = LoggerFake()
    opensearch, _ = opensearch_mock("a" * 64)
    handler = SentenceSimilarityHandler(
        service_config, SessionSequence([]), opensearch, SqsFake(), logger
    )
    sentence_records = [
        SimpleNamespace(
            word_count=5,
            is_non_matcher=False,
            is_tracer=False,
            is_form_language=False,
        ),
        SimpleNamespace(
            word_count=2,
            is_non_matcher=False,
            is_tracer=False,
            is_form_language=False,
        ),
        SimpleNamespace(
            word_count=8,
            is_non_matcher=True,
            is_tracer=False,
            is_form_language=True,
        ),
    ]

    eligible_records = handler._select_eligible_source_sentences(sentence_records)

    assert eligible_records == sentence_records[:1]
    assert "Short sentences: 1. Classification skips: 1" in logger.messages[-1][1]
