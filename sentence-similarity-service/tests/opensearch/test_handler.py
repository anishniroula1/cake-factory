import pytest

from similarity_completed.opensearch.errors import PermanentDataError
from similarity_completed.opensearch.handlers.sentence_similarity_handler import SentenceSimilarityHandler
from tests.opensearch.fakes import LoggerFake, SessionSequence, SqsFake, opensearch_mock, session_mock


def source_record(sentence_key, classification=None):
    return {"sentence_key": sentence_key, "vector": [0.1, 0.2], "classification": classification}


def test_non_matcher_skips_catalog_search_without_creating_match_row(service_config, sentence_row):
    classification = {"is_non_matcher": True, "is_tracer": False, "is_form_language": False}
    classification_update = session_mock()
    sessions = SessionSequence([classification_update])
    opensearch, router = opensearch_mock(
        sentence_row.sentence_key, saved_classification=classification, tracer_result=classification
    )
    handler = SentenceSimilarityHandler(service_config, sessions, opensearch, SqsFake(), LoggerFake())
    record = source_record(sentence_row.sentence_key, classification)

    result = handler._process_sentence(sentence_row, "catalog-asylee", record)

    assert result == {"is_non_matcher": True, "saved_relationships": 0}
    assert classification_update.execute.call_count == 1
    classification_update.commit.assert_called_once_with()
    assert router.semantic_search_count == 0
    assert sessions.sessions == []


@pytest.mark.parametrize(
    "classification",
    [
        {"is_non_matcher": False, "is_tracer": True, "is_form_language": False},
        {"is_non_matcher": False, "is_tracer": False, "is_form_language": True},
    ],
)
def test_tracer_and_form_language_skip_catalog_search(service_config, sentence_row, classification):
    sessions = SessionSequence([session_mock()])
    opensearch, router = opensearch_mock(sentence_row.sentence_key, tracer_result=classification)
    handler = SentenceSimilarityHandler(service_config, sessions, opensearch, SqsFake(), LoggerFake())

    result = handler._process_sentence(sentence_row, "catalog-asylee", source_record(sentence_row.sentence_key))

    assert result == {"is_non_matcher": True, "saved_relationships": 0}
    assert router.semantic_search_count == 0
    opensearch.update.assert_called_once()
    assert sessions.sessions == []


def test_missing_classification_is_calculated_and_saved(service_config, sentence_row):
    opensearch, router = opensearch_mock(sentence_row.sentence_key)
    handler = SentenceSimilarityHandler(service_config, SessionSequence([]), opensearch, SqsFake(), LoggerFake())
    record = source_record(sentence_row.sentence_key)

    classification = handler.tracer_filtering_service.get_sentence_classification("catalog-asylee", record, "Asylee")

    assert classification["is_non_matcher"] is False
    assert record["classification"] == classification
    assert len(router.saved_classifications) == 1


def test_saved_non_matcher_is_rechecked(service_config, sentence_row):
    non_matcher = {"is_non_matcher": True, "is_tracer": False, "is_form_language": False}
    opensearch, router = opensearch_mock(sentence_row.sentence_key, tracer_result=non_matcher)
    handler = SentenceSimilarityHandler(service_config, SessionSequence([]), opensearch, SqsFake(), LoggerFake())
    record = source_record(sentence_row.sentence_key, non_matcher)

    classification = handler.tracer_filtering_service.get_sentence_classification("catalog-asylee", record, "Asylee")

    assert classification["is_non_matcher"] is True
    assert len(router.saved_classifications) == 1


def test_all_missing_catalog_keys_are_reported(service_config, sentence_row):
    opensearch, _ = opensearch_mock(sentence_row.sentence_key, missing_catalog_keys={"KEY-B", "KEY-C"})
    handler = SentenceSimilarityHandler(service_config, SessionSequence([]), opensearch, SqsFake(), LoggerFake())
    source = source_record(sentence_row.sentence_key)

    with pytest.raises(PermanentDataError) as error:
        handler.tracer_filtering_service.find_eligible_match_targets(
            sentence_row, "catalog-asylee", {"KEY-B": 0.1, "KEY-C": 0.2}, source
        )

    assert str(error.value) == "catalog vectors are missing for sentence keys: KEY-B, KEY-C"


def test_empty_document_is_rejected(service_config):
    opensearch, _ = opensearch_mock("a" * 64)
    handler = SentenceSimilarityHandler(
        service_config, SessionSequence([session_mock(scalar_value=0)]), opensearch, SqsFake(), LoggerFake()
    )

    with pytest.raises(PermanentDataError):
        handler.process(10)


def test_sentence_count_uses_only_sentences_vector_tsp_id(service_config):
    count_session = session_mock(scalar_value=25)
    opensearch, _ = opensearch_mock("a" * 64)
    handler = SentenceSimilarityHandler(
        service_config, SessionSequence([count_session]), opensearch, SqsFake(), LoggerFake()
    )

    assert handler.sentence_vector_db_store.count_sentences_by_tsp_id(10) == 25
    sql = str(count_session.scalar.call_args.args[0])
    assert "JOIN" not in sql.upper()
    assert "is_current" not in sql
    assert "sentences_vector.tsp_id" in sql
