from pathlib import Path

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from sentence_similarity.models import TitanMatches


def test_titan_matches_model_contains_final_table_definition():
    table_sql = str(
        CreateTable(TitanMatches.__table__).compile(
            dialect=postgresql.dialect()
        )
    )
    index_sql = {
        str(CreateIndex(index).compile(dialect=postgresql.dialect()))
        for index in TitanMatches.__table__.indexes
    }

    assert "PRIMARY KEY (global_id, match_bucket_number)" in table_sql
    assert "GENERATED ALWAYS AS" in table_sql
    assert "tsp.titan_match_object_size(matches)" in table_sql
    assert "tsp.titan_verbatim_global_ids(matches)" in table_sql
    assert (
        "CREATE INDEX ix_titan_matches_tsp_id "
        "ON tsp.titan_matches (tsp_id, global_id)"
    ) in index_sql


def test_titan_match_function_does_not_create_an_empty_row():
    sql_file = Path(__file__).parents[1] / "sql/002_titan_match_buckets.sql"
    sql = sql_file.read_text(encoding="utf-8")

    assert "IF processed_match_count = 0 THEN\n        RETURN 0;" in sql
