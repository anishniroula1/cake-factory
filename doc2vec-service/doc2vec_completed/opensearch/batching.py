from collections.abc import Iterator


def batches[Value](values: list[Value], size: int) -> Iterator[list[Value]]:
    """Yield small lists without copying the entire input again."""

    for offset in range(0, len(values), size):
        yield values[offset : offset + size]
