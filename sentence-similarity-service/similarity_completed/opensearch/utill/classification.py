from similarity_completed.opensearch.contracts import Classification
from similarity_completed.opensearch.utill.constants import CLASSIFICATION_FIELDS


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
