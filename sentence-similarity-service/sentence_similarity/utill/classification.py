from sentence_similarity.contracts import Classification
from sentence_similarity.utill.constants import CLASSIFICATION_FIELDS


def saved_classification(source: dict) -> Classification | None:
    """Read the three reusable flags from a sentence catalog document.

    Input:
        {isNonMatcher: True, isTracer: True, isFormLanguage: False}

    Output:
        The same flags using Python names. Closest ID and score are None
        because those values exist only in sentences_vector.

    Output is None when any flag is missing. That tells Sentence Similarity to
    run the tracer search and complete the classification first.
    """

    # All three fields must exist before this catalog decision can be reused.
    if not all(field in source for field in CLASSIFICATION_FIELDS):
        return None
    return {
        "is_non_matcher": bool(source["isNonMatcher"]),
        # Closest-match details belong to sentences_vector, not this catalog.
        "closest_non_matcher": None,
        "closest_nn_score": None,
        "is_tracer": bool(source["isTracer"]),
        "is_form_language": bool(source["isFormLanguage"]),
    }
