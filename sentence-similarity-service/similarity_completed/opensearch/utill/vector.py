import base64
import binascii
import struct

from similarity_completed.opensearch.errors import PermanentDataError


def decode_vector(encoded_vector: str, vector_dimension: int) -> list[float]:
    """Decode a Base64 OpenSearch doc-value vector into float32 values. The
    decoded value must contain four bytes for every expected float.

    Input: encoded_vector="AACAPwAAAL8=", vector_dimension=2
    Output: [1.0, -0.5]

    Production Titan vectors use vector_dimension=512.

    Raises:
        PermanentDataError: If the value is invalid Base64 or has an invalid size.
    """

    try:
        vector_bytes = base64.b64decode(encoded_vector, validate=True)
    except (binascii.Error, ValueError) as error:
        raise PermanentDataError("OpenSearch returned an invalid catalog vector") from error
    # Titan returns float32 values, so a 512-dimension vector needs 2,048 bytes.
    expected_bytes = vector_dimension * 4
    if len(vector_bytes) != expected_bytes:
        raise PermanentDataError(f"catalog vector has {len(vector_bytes)} bytes; expected {expected_bytes}")
    return list(struct.unpack(f"<{vector_dimension}f", vector_bytes))


def validate_vector(vector: list[float], vector_dimension: int) -> None:
    """Validate a vector against the configured OpenSearch dimension. This
    produces a clear service error before an invalid vector reaches OpenSearch.

    Input: vector=[512 float values], vector_dimension=512

    Raises:
        PermanentDataError: If the vector dimension does not match the index mapping.
    """

    if len(vector) != vector_dimension:
        raise PermanentDataError(f"vector has {len(vector)} values; expected {vector_dimension}")
