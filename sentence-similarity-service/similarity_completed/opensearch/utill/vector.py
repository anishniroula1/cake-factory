import base64
import binascii
import struct

from similarity_completed.opensearch.errors import PermanentDataError


def decode_vector(encoded_vector: str, vector_dimension: int) -> list[float]:
    """Convert one Base64 binary OpenSearch vector into Python float values.

    Input:
        encoded_vector=Base64 binary returned by docvalue_fields
        vector_dimension=512

    Output:
        A list containing exactly 512 float values.

    Each float uses four bytes. Invalid Base64 or the wrong byte count stops
    processing before an invalid vector is sent into another search.
    """

    try:
        vector_bytes = base64.b64decode(encoded_vector, validate=True)
    except (binascii.Error, ValueError) as error:
        raise PermanentDataError(
            "OpenSearch returned an invalid catalog vector"
        ) from error
    # Titan returns float32 values, so a 512-dimension vector needs 2,048 bytes.
    expected_bytes = vector_dimension * 4
    if len(vector_bytes) != expected_bytes:
        raise PermanentDataError(
            f"catalog vector has {len(vector_bytes)} bytes; "
            f"expected {expected_bytes}"
        )
    return list(struct.unpack(f"<{vector_dimension}f", vector_bytes))


def validate_vector(vector: list[float], vector_dimension: int) -> None:
    """Confirm that a vector has the dimension required by OpenSearch.

    Input: vector has 512 values and vector_dimension=512
    Output: No value is returned; processing continues.

    Input: vector has 1,024 values and vector_dimension=512
    Output: PermanentDataError because OpenSearch would reject the query.
    """

    if len(vector) != vector_dimension:
        raise PermanentDataError(
            f"vector has {len(vector)} values; expected {vector_dimension}"
        )
