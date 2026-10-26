from similarity_completed.opensearch.constants import CLASSIFICATION_FIELDS
from similarity_completed.opensearch.contracts import Classification


def saved_classification(catalog_source: dict) -> Classification | None:
    """Extract a complete classification from a catalog document. All three
    flags must exist before the classification can be reused.

    Input: catalog_source={"isNonMatcher": True, "isTracer": False,
    "isFormLanguage": False}
    Output: {"is_non_matcher": True, "is_tracer": False,
    "is_form_language": False}

    The method returns None when any required field is missing.
    """

    # All three fields must exist before this catalog decision can be reused.
    if not all(field in catalog_source for field in CLASSIFICATION_FIELDS):
        return None
    return {
        "is_non_matcher": bool(catalog_source["isNonMatcher"]),
        "is_tracer": bool(catalog_source["isTracer"]),
        "is_form_language": bool(catalog_source["isFormLanguage"]),
    }
