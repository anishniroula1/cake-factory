from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from similarity_completed.opensearch.models import TitanMatches


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
    assert "application_id INTEGER NOT NULL" in table_sql
    assert (
        "CREATE INDEX ix_titan_matches_tsp_id "
        "ON tsp.titan_matches (tsp_id, global_id)"
    ) in index_sql
    assert (
        "CREATE INDEX ix_titan_matches_application_id "
        "ON tsp.titan_matches (application_id, global_id)"
    ) in index_sql
