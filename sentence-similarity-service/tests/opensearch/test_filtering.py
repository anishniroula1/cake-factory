import pytest

from similarity_completed.opensearch.utill.filtering import (
    derive_is_tracer,
    should_skip_sentence_catalog_search,
)


@pytest.mark.parametrize(
    ("match_found", "is_non_matcher", "is_form_language", "expected"),
    [
        (False, False, False, False),
        (True, False, False, True),
        (True, True, False, False),
        (True, True, True, False),
        (True, False, True, False),
    ],
)
def test_is_tracer_is_derived_from_the_two_reference_flags(
    match_found,
    is_non_matcher,
    is_form_language,
    expected,
):
    assert (
        derive_is_tracer(
            match_found,
            is_non_matcher,
            is_form_language,
        )
        is expected
    )


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
    is_non_matcher,
    is_tracer,
    is_form_language,
    expected,
):
    classification = {
        "is_non_matcher": is_non_matcher,
        "closest_non_matcher": None,
        "closest_nn_score": None,
        "is_tracer": is_tracer,
        "is_form_language": is_form_language,
    }

    assert should_skip_sentence_catalog_search(classification) is expected
