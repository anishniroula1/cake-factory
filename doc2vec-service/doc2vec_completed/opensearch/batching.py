from collections.abc import Iterator


def batches[Value](values: list[Value], size: int) -> Iterator[list[Value]]:
    """Break one large list into smaller lists.

    Input: 65 rows with size=30.
    Output: lists containing 30 rows, 30 rows, and 5 rows.
    """

    for offset in range(0, len(values), size):
        yield values[offset : offset + size]
