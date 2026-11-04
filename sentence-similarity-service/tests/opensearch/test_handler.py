import pytest
from sqlalchemy.dialects import postgresql

from similarity_completed.opensearch.handlers import sentence_similarity_handler as handler_module
from similarity_completed.opensearch.handlers.sentence_similarity_handler import SentenceSimilarityHandler
from tests.opensearch.fakes import LoggerFake, SessionSequence, SqsFake, opensearch_mock, session_mock


def source_record(sentence_key, classification=None, word_count=5):
    return {
        "sentence_key": sentence_key,
        "word_count": word_count,
        "vector": [0.1, 0.2],
        "classification": classification,
    }


def test_non_matcher_skips_catalog_search_without_creating_match_row(service_config, sentence_row):
    classification = {"is_non_matcher": True, "is_tracer": False, "is_form_language": False}
    classification_update = session_mock(rows=[sentence_row.global_id])
    sessions = SessionSequence([classification_update])
    opensearch, router = opensearch_mock(
        sentence_row.sentence_key, saved_classification=classification, tracer_result=classification
    )
    handler = SentenceSimilarityHandler(service_config, sessions, opensearch, SqsFake(), LoggerFake())
    record = source_record(sentence_row.sentence_key, classification)

    result = handler._process_source_sentence(sentence_row, "catalog-asylee", record)

    assert result == {**classification, "match_relationships": []}
    lock_statement = str(classification_update.scalars.call_args.args[0].compile(dialect=postgresql.dialect()))
    assert "ORDER BY" in lock_statement and "FOR NO KEY UPDATE" in lock_statement
    assert router.semantic_search_count == 0


@pytest.mark.parametrize(
    "classification",
    [
        {"is_non_matcher": False, "is_tracer": True, "is_form_language": False},
        {"is_non_matcher": False, "is_tracer": False, "is_form_language": True},
    ],
)
def test_tracer_and_form_language_skip_catalog_search(service_config, sentence_row, classification):
    sessions = SessionSequence([session_mock(rows=[sentence_row.global_id])])
    opensearch, router = opensearch_mock(sentence_row.sentence_key, tracer_result=classification)
    handler = SentenceSimilarityHandler(service_config, sessions, opensearch, SqsFake(), LoggerFake())

    result = handler._process_source_sentence(sentence_row, "catalog-asylee", source_record(sentence_row.sentence_key))

    assert result == {**classification, "match_relationships": []}
    assert router.semantic_search_count == 0


def test_short_sentence_is_classified_but_skips_catalog_matching(service_config, sentence_row):
    opensearch, router = opensearch_mock(sentence_row.sentence_key)
    sessions = SessionSequence([session_mock(rows=[sentence_row.global_id])])
    handler = SentenceSimilarityHandler(service_config, sessions, opensearch, SqsFake(), LoggerFake())

    result = handler._process_source_sentence(
        sentence_row, "catalog-asylee", source_record(sentence_row.sentence_key, word_count=4)
    )

    assert result["match_relationships"] == []
    assert router.semantic_search_count == 0


def test_missing_classification_is_calculated_and_saved(service_config, sentence_row):
    opensearch, router = opensearch_mock(sentence_row.sentence_key)
    handler = SentenceSimilarityHandler(service_config, SessionSequence([]), opensearch, SqsFake(), LoggerFake())
    record = source_record(sentence_row.sentence_key)

    classification = handler.tracer_filtering_service.resolve_sentence_classification(
        "catalog-asylee", record, "Asylee", sentence_row.global_id
    )

    assert record["classification"] == classification and len(router.saved_classifications) == 1


def test_saved_non_matcher_is_reused(service_config, sentence_row):
    non_matcher = {"is_non_matcher": True, "is_tracer": False, "is_form_language": False}
    opensearch, router = opensearch_mock(sentence_row.sentence_key, tracer_result=non_matcher)
    handler = SentenceSimilarityHandler(service_config, SessionSequence([]), opensearch, SqsFake(), LoggerFake())
    record = source_record(sentence_row.sentence_key, non_matcher)

    classification = handler.tracer_filtering_service.resolve_sentence_classification(
        "catalog-asylee", record, "Asylee", sentence_row.global_id
    )

    assert classification["is_non_matcher"] is True and router.saved_classifications == []


def test_all_missing_catalog_keys_stop_the_current_message(service_config, sentence_row):
    opensearch, _ = opensearch_mock(sentence_row.sentence_key, missing_catalog_keys={"KEY-B", "KEY-C"})
    logger = LoggerFake()
    handler = SentenceSimilarityHandler(service_config, SessionSequence([]), opensearch, SqsFake(), logger)
    source_catalog_record = source_record(sentence_row.sentence_key)

    result = handler.tracer_filtering_service.find_eligible_target_occurrences(
        sentence_row, "catalog-asylee", {"KEY-B": 0.1, "KEY-C": 0.2}, source_catalog_record
    )

    assert result is None


def test_empty_document_stops_without_forwarding(service_config):
    opensearch, _ = opensearch_mock("a" * 64)
    sqs = SqsFake()
    handler = SentenceSimilarityHandler(
        service_config, SessionSequence([session_mock(scalar_value=0)]), opensearch, sqs, LoggerFake()
    )

    assert handler.process_document_sentences(10) is None


def test_occurrence_readiness_waits_once_and_then_continues(monkeypatch, service_config):
    waits = []
    opensearch, _ = opensearch_mock("a" * 64, occurrence_counts=[0, 1])
    handler = SentenceSimilarityHandler(service_config, SessionSequence([]), opensearch, SqsFake(), LoggerFake())
    monkeypatch.setattr(handler_module.time, "sleep", waits.append)

    assert handler._wait_for_document_occurrences(10) is True
    assert waits == [30]


def test_sentence_count_uses_only_sentences_vector_tsp_id(service_config):
    count_session = session_mock(scalar_value=25)
    opensearch, _ = opensearch_mock("a" * 64)
    handler = SentenceSimilarityHandler(
        service_config, SessionSequence([count_session]), opensearch, SqsFake(), LoggerFake()
    )

    count = handler.sentence_vector_db_store.count_document_sentences(10)
    sql = str(count_session.scalar.call_args.args[0])

    assert count == 25 and "JOIN" not in sql.upper()
