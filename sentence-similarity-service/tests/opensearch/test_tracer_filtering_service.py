import pytest

from similarity_completed.opensearch.tracer_filtering_service import TracerFilteringService


@pytest.mark.parametrize(
    ("is_non_matcher", "is_tracer", "is_form_language", "expected"),
    [
        (False, False, False, False),
        (True, False, False, True),
        (True, False, True, True),
        (False, False, True, True),
        (False, True, False, True),
    ],
)
def test_every_tracer_catalog_classification_skips_sentence_matching(
    is_non_matcher, is_tracer, is_form_language, expected
):
    classification = {"is_non_matcher": is_non_matcher, "is_tracer": is_tracer, "is_form_language": is_form_language}

    assert TracerFilteringService.should_skip_sentence_catalog_search(classification) is expected
