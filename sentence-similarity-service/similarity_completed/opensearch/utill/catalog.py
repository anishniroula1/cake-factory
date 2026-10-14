from similarity_completed.opensearch import constants
from similarity_completed.opensearch.errors import PermanentDataError


def select_catalog_alias(analysis_group: str) -> str:
    """Select the sentence catalog alias for an analysis group.

    Input: analysis_group="Asylee"
    Output: "sentence_semantic_catalog_asylee"

    Asylee and Refugee sentences use separate trained IVF-SQ16 catalogs.

    Raises:
        PermanentDataError: If the analysis group is unsupported.
    """

    # Normalize values such as ASYLEE, asylee, and Asylee to the same group.
    normalized_group = analysis_group.strip().casefold()
    if normalized_group == "asylee":
        return constants.ASYLEE_CATALOG_ALIAS
    if normalized_group == "refugee":
        return constants.REFUGEE_CATALOG_ALIAS
    raise PermanentDataError(f"unsupported analysis_group {analysis_group!r}")
