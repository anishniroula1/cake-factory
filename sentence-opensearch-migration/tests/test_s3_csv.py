import io

import pytest

from sentence_migration.s3_csv import S3TspIdReader


class FakeS3:
    def __init__(self, content: str):
        self.content = content
        self.calls = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)
        return {"Body": io.BytesIO(self.content.encode("utf-8"))}


def test_s3_csv_streams_tsp_ids():
    s3 = FakeS3("tsp_id\nTSP-9\n002\n")
    reader = S3TspIdReader(s3, "migration-bucket")

    assert list(reader.tsp_ids("folder/ids.csv")) == ["TSP-9", "002"]
    assert s3.calls == [{"Bucket": "migration-bucket", "Key": "folder/ids.csv"}]


@pytest.mark.parametrize(
    "csv_content",
    [
        "global_id\n1\n",
        "tsp_ids\nT1\n",
        "tsp_id,ignored\nT1,x\n",
        " tsp_id\nT1\n",
    ],
)
def test_s3_csv_requires_exactly_one_tsp_id_header(csv_content):
    reader = S3TspIdReader(FakeS3(csv_content), "bucket")

    with pytest.raises(ValueError, match="exactly one column.*tsp_id"):
        list(reader.tsp_ids("ids.csv"))


def test_s3_csv_rejects_extra_row_columns():
    reader = S3TspIdReader(FakeS3("tsp_id\nT1,unexpected\n"), "bucket")

    with pytest.raises(ValueError, match="row 2 contains more than one column"):
        list(reader.tsp_ids("ids.csv"))


def test_s3_csv_rejects_duplicate_tsp_id():
    reader = S3TspIdReader(FakeS3("tsp_id\nT1\nT1\n"), "bucket")

    with pytest.raises(ValueError, match="Duplicate TSP ID.*row 3"):
        list(reader.tsp_ids("ids.csv"))
