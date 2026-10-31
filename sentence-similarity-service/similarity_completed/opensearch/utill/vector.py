import math

from similarity_completed.opensearch.errors import PermanentDataError


def read_catalog_vector(vector_value: object, vector_dimension: int) -> list[float]:
    """Read a numeric vector returned in an OpenSearch document source.

    Input: vector_value=[0.25, -0.5], vector_dimension=2
    Output: [0.25, -0.5]

    Production Titan vectors contain 512 finite numeric values. IVF-SQ16
    compresses the internal search index, but the document source still
    contains the numeric vector originally indexed.

    Raises:
        PermanentDataError: If the value is not a finite numeric array with the
            expected dimension.
    """

    if not isinstance(vector_value, list):
        raise PermanentDataError("catalog vector is not a numeric array")

    if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in vector_value):
        raise PermanentDataError("catalog vector contains a non-numeric value")

    vector = [float(value) for value in vector_value]
    validate_vector(vector, vector_dimension)
    if any(not math.isfinite(value) for value in vector):
        raise PermanentDataError("catalog vector contains a non-finite value")
    return vector


def validate_vector(vector: list[float], vector_dimension: int) -> None:
    """Validate a vector against the configured OpenSearch dimension. This
    produces a clear service error before an invalid vector reaches OpenSearch.

    Input: vector=[512 float values], vector_dimension=512

    Raises:
        PermanentDataError: If the vector dimension does not match the index mapping.
    """

    if len(vector) != vector_dimension:
        raise PermanentDataError(f"vector has {len(vector)} values; expected {vector_dimension}")
