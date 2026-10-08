from similarity_completed.opensearch.contracts import Classification
from similarity_completed.opensearch.utill.constants import CLASSIFICATION_FIELDS


def derive_is_tracer(tracer_match_found: bool, is_non_matcher: bool, is_form_language: bool) -> bool:
    """Return true when a matched reference is only a tracer sentence."""

    return tracer_match_found and not is_non_matcher and not is_form_language


def saved_classification(source: dict) -> Classification | None:
    """Read the three reusable flags from a sentence catalog document.

    Input:
        {isNonMatcher: True, isTracer: True, isFormLanguage: False}

    Output:
        The same flags using Python names.

    Output is None when any flag is missing. That tells Sentence Similarity to
    run the tracer search and complete the classification first.
    """

    # All three fields must exist before this catalog decision can be reused.
    if not all(field in source for field in CLASSIFICATION_FIELDS):
        return None
    return {
        "is_non_matcher": bool(source["isNonMatcher"]),
        "is_tracer": bool(source["isTracer"]),
        "is_form_language": bool(source["isFormLanguage"]),
    }
