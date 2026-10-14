from similarity_completed.opensearch.contracts import Classification
from similarity_completed.opensearch.utill.constants import CLASSIFICATION_FIELDS


def saved_classification(source: dict) -> Classification | None:
    """Extract a complete classification from a catalog document. All three
    flags must exist before the classification can be reused.

    Input: source={"isNonMatcher": True, "isTracer": False,
    "isFormLanguage": False}
    Output: {"is_non_matcher": True, "is_tracer": False,
    "is_form_language": False}

    The method returns None when any required field is missing.
    """

    # All three fields must exist before this catalog decision can be reused.
    if not all(field in source for field in CLASSIFICATION_FIELDS):
        return None
    return {
        "is_non_matcher": bool(source["isNonMatcher"]),
        "is_tracer": bool(source["isTracer"]),
        "is_form_language": bool(source["isFormLanguage"]),
    }
