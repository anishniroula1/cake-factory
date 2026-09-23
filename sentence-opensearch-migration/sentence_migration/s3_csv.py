import csv
import io
from collections.abc import Iterator

TSP_HEADER = "tsp_id"


class S3TspIdReader:
    def __init__(self, s3_client, bucket: str):
        self.s3_client = s3_client
        self.bucket = bucket

    def tsp_ids(self, object_key: str) -> Iterator[str]:
        """Stream unique TSP IDs from one S3 CSV object."""

        response = self.s3_client.get_object(Bucket=self.bucket, Key=object_key)
        body = response["Body"]
        text_stream = io.TextIOWrapper(body, encoding="utf-8-sig", newline="")
        try:
            reader = csv.DictReader(text_stream)
            headers = reader.fieldnames or []
            if headers != [TSP_HEADER]:
                raise ValueError(
                    "S3 CSV must contain exactly one column with the header tsp_id"
                )
            seen = set()
            for line_number, row in enumerate(reader, start=2):
                if None in row:
                    raise ValueError(
                        f"S3 CSV row {line_number} contains more than one column"
                    )
                tsp_id = (row.get(TSP_HEADER) or "").strip()
                if not tsp_id:
                    raise ValueError(f"Empty TSP ID at CSV row {line_number}")
                if len(tsp_id) > 512:
                    raise ValueError(f"TSP ID at CSV row {line_number} is too long")
                if tsp_id in seen:
                    raise ValueError(
                        f"Duplicate TSP ID {tsp_id!r} at CSV row {line_number}"
                    )
                seen.add(tsp_id)
                yield tsp_id
        finally:
            text_stream.close()
