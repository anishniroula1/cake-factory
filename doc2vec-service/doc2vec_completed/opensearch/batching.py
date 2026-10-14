from collections.abc import Iterator


def batches[Value](values: list[Value], size: int) -> Iterator[list[Value]]:
    """Split a list into ordered batches without dropping remaining items.

    Input: values=[65 rows], size=30
    Output: lists containing 30 rows, 30 rows, and 5 rows.
    """

    for offset in range(0, len(values), size):
        yield values[offset : offset + size]
