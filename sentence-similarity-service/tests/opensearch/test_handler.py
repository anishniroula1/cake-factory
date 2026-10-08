import pytest

from similarity_completed.opensearch.errors import PermanentDataError
from similarity_completed.opensearch.handlers import sentence_similarity_handler as handler_module
from similarity_completed.opensearch.handlers.sentence_similarity_handler import SentenceSimilarityHandler
from tests.opensearch.fakes import LoggerFake, OpenSearchFake, SessionFake, SessionSequence, SqsFake


def source_record(sentence_key, classification=None):
    return {"sentence_key": sentence_key, "vector": [0.1, 0.2], "classification": classification}


class NonMatcherOpenSearch(OpenSearchFake):
    def catalog_matches(self, catalog_alias, source_key, vector):
        raise AssertionError("non-matcher must not enter semantic search")


class ExcludedTracerOpenSearch(OpenSearchFake):
    def __init__(self, sentence_key, classification):
        super().__init__(sentence_key)
        self.classification = classification

    def tracer_classification(self, vector, analysis_group):
        return self.classification

    def catalog_matches(self, catalog_alias, source_key, vector):
        raise AssertionError("excluded sentence must not enter catalog search")


@pytest.fixture(autouse=True)
def use_fake_opensearch_queries(monkeypatch):
    """Route handler query functions through the small OpenSearch test fake."""

    def select_alias(config, analysis_group):
        return f"catalog-{analysis_group.casefold()}"

    def load_records_by_sentence_keys(store, catalog_alias, sentence_keys):
        return store.client.catalog_records(catalog_alias, sentence_keys)

    def classify_sentence_by_vector(store, vector, analysis_group):
        return store.client.tracer_classification(vector, analysis_group)

    def update_classification_by_sentence_key(store, catalog_alias, sentence_key, classification):
        store.client.save_catalog_classification(catalog_alias, sentence_key, classification)

    def search_semantic_matches_by_vector(store, catalog_alias, source_key, vector):
        return store.client.catalog_matches(catalog_alias, source_key, vector)

    def search_matching_occurrences_outside_application(store, key_distances, analysis_group, source_application_id):
        return store.client.target_occurrences(key_distances, analysis_group, source_application_id)

    monkeypatch.setattr(handler_module, "select_catalog_alias", select_alias)
    monkeypatch.setattr(
        handler_module.SentenceCatalogOpenSearchStore, "load_records_by_sentence_keys", load_records_by_sentence_keys
    )
    monkeypatch.setattr(
        handler_module.TracerOpenSearchStore, "classify_sentence_by_vector", classify_sentence_by_vector
    )
    monkeypatch.setattr(
        handler_module.SentenceCatalogOpenSearchStore,
        "update_classification_by_sentence_key",
        update_classification_by_sentence_key,
    )
    monkeypatch.setattr(
        handler_module.SentenceCatalogOpenSearchStore,
        "search_semantic_matches_by_vector",
        search_semantic_matches_by_vector,
    )
    monkeypatch.setattr(
        handler_module.OccurrenceOpenSearchStore,
        "search_matching_occurrences_outside_application",
        search_matching_occurrences_outside_application,
    )


def test_complete_document_flow_saves_bucket_matches_and_publishes(service_config, sentence_row):
    allowed = {"is_non_matcher": False, "is_tracer": False, "is_form_language": False}
    source_update = SessionFake()
    target_update = SessionFake()
    match_write = SessionFake(execute_value=1)
    sessions = SessionSequence(
        [
            SessionFake(scalar_value=1),
            SessionFake(rows=[sentence_row]),
            source_update,
            target_update,
            match_write,
            SessionFake(rows=[]),
        ]
    )
    opensearch = OpenSearchFake(sentence_row.sentence_key)
    sqs = SqsFake()
    handler = SentenceSimilarityHandler(service_config, sessions, opensearch, sqs, LoggerFake())

    result = handler.process(10)

    assert result == {
        "tsp_id": 10,
        "source_count": 1,
        "processed_count": 1,
        "non_matcher_count": 0,
        "saved_relationship_count": 1,
    }
    assert source_update.commits == 1
    assert target_update.commits == 1
    assert match_write.commits == 1
    parameters = match_write.executed[0][1]
    assert parameters["source_global_id"] == 100
    assert parameters["source_application_id"] == 1
    assert parameters["matches"] == {"200": {"tsp_id": 20, "application_id": 2, "distance": 0.0}}
    assert sqs.sent[0]["message_body"] == {"tsp_id": 10}
    assert sqs.sent[0]["send_queue_url"] == "next.fifo"
    assert (
        allowed
        == opensearch.catalog_records("catalog-asylee", [sentence_row.sentence_key])[sentence_row.sentence_key][
            "classification"
        ]
    )


def test_non_matcher_skips_catalog_search_without_creating_match_row(service_config, sentence_row):
    classification_update = SessionFake()
    sessions = SessionSequence([classification_update])
    opensearch = NonMatcherOpenSearch(sentence_row.sentence_key, non_matcher=True)
    handler = SentenceSimilarityHandler(service_config, sessions, opensearch, SqsFake(), LoggerFake())
    record = opensearch.catalog_records("catalog-asylee", [sentence_row.sentence_key])[sentence_row.sentence_key]

    result = handler._process_sentence(sentence_row, "catalog-asylee", record)

    assert result == {"is_non_matcher": True, "saved_relationships": 0}
    assert len(classification_update.executed) == 1
    assert classification_update.commits == 1
    assert sessions.sessions == []


@pytest.mark.parametrize(
    "classification",
    [
        {"is_non_matcher": False, "is_tracer": True, "is_form_language": False},
        {"is_non_matcher": False, "is_tracer": False, "is_form_language": True},
    ],
)
def test_tracer_and_form_language_skip_catalog_search(service_config, sentence_row, classification):
    sessions = SessionSequence([SessionFake()])
    handler = SentenceSimilarityHandler(
        service_config,
        sessions,
        ExcludedTracerOpenSearch(sentence_row.sentence_key, classification),
        SqsFake(),
        LoggerFake(),
    )

    result = handler._process_sentence(sentence_row, "catalog-asylee", source_record(sentence_row.sentence_key))

    assert result == {"is_non_matcher": True, "saved_relationships": 0}
    assert sessions.sessions == []


def test_missing_classification_is_calculated_and_saved(service_config, sentence_row):
    opensearch = OpenSearchFake(sentence_row.sentence_key)
    handler = SentenceSimilarityHandler(service_config, SessionSequence([]), opensearch, SqsFake(), LoggerFake())
    record = source_record(sentence_row.sentence_key)

    classification = handler.tracer_filtering_service.get_sentence_classification("catalog-asylee", record, "Asylee")

    assert classification["is_non_matcher"] is False
    assert record["classification"] == classification
    assert len(opensearch.saved_classifications) == 1


def test_saved_non_matcher_is_rechecked(service_config, sentence_row):
    opensearch = OpenSearchFake(sentence_row.sentence_key, non_matcher=True)
    handler = SentenceSimilarityHandler(service_config, SessionSequence([]), opensearch, SqsFake(), LoggerFake())
    record = source_record(
        sentence_row.sentence_key, {"is_non_matcher": True, "is_tracer": False, "is_form_language": False}
    )

    classification = handler.tracer_filtering_service.get_sentence_classification("catalog-asylee", record, "Asylee")

    assert classification["is_non_matcher"] is True
    assert len(opensearch.saved_classifications) == 1


def test_empty_document_is_rejected(service_config):
    handler = SentenceSimilarityHandler(
        service_config,
        SessionSequence([SessionFake(scalar_value=0)]),
        OpenSearchFake("a" * 64),
        SqsFake(),
        LoggerFake(),
    )

    with pytest.raises(PermanentDataError):
        handler.process(10)


def test_sentence_count_uses_only_sentences_vector_tsp_id(service_config):
    count_session = SessionFake(scalar_value=25)
    handler = SentenceSimilarityHandler(
        service_config, SessionSequence([count_session]), OpenSearchFake("a" * 64), SqsFake(), LoggerFake()
    )

    assert handler.sentence_vector_db_store.count_sentences_by_tsp_id(10) == 25
    sql = str(count_session.scalar_statements[0])
    assert "JOIN" not in sql.upper()
    assert "is_current" not in sql
    assert "sentences_vector.tsp_id" in sql
