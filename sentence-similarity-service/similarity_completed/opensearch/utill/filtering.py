from similarity_completed.opensearch.contracts import Classification


def derive_is_tracer(
    tracer_match_found: bool,
    is_non_matcher: bool,
    is_form_language: bool,
) -> bool:
    """Decide whether a tracer-catalog result is a tracer sentence.

    The tracer table does not have an is_tracer column. A matching reference
    is a tracer when both saved flags are false.

    Input: match found, is_non_matcher=False, is_form_language=False
    Output: True

    Input: no match found, is_non_matcher=False, is_form_language=False
    Output: False, because the sentence was not found in the tracer catalog.
    """

    return tracer_match_found and not is_non_matcher and not is_form_language


def should_skip_sentence_catalog_search(
    classification: Classification,
) -> bool:
    """Return true when a sentence must not enter normal sentence matching.

    These tracer-table flag combinations stop matching:

    Input: is_non_matcher=True, is_form_language=False
    Input: is_non_matcher=True, is_form_language=True
    Input: is_non_matcher=False, is_form_language=True

    A derived tracer also stops matching. Only a sentence with all three
    classification flags set to false can search the sentence catalog.
    """

    return (
        classification["is_tracer"]
        or classification["is_non_matcher"]
        or classification["is_form_language"]
    )
