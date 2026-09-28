def batches(values: list, size: int):
    """Yield small lists without copying the entire input again."""

    for offset in range(0, len(values), size):
        yield values[offset : offset + size]

