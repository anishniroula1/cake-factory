from collections.abc import Iterator


def batches[Value](values: list[Value], size: int) -> Iterator[list[Value]]:
    """Split a list into ordered batches without dropping remaining items.

    Input: values=[65 records], size=30
    Output: lists containing 30 records, 30 records, and 5 records.
    """

    for offset in range(0, len(values), size):
        yield values[offset : offset + size]

