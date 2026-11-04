import pytest
from sqlalchemy.exc import IntegrityError

from similarity_completed.opensearch.db_store.titan_matches_db_store import TitanMatchesDBStore
from similarity_completed.opensearch.errors import PermanentDataError
from tests.opensearch.fakes import LoggerFake, SessionSequence, session_mock


class PostgresDiagnostic:
    message_primary = (
        'insert or update on table "titan_matches" violates foreign key constraint '
        '"fk_titan_matches_sentence_vector"'
    )
    message_detail = 'Key (global_id)=(50001) is not present in table "sentences_vector".'


class ForeignKeyViolation(Exception):
    diag = PostgresDiagnostic()


def test_foreign_key_failure_reports_missing_row_without_query_or_match_input():
    database_error = IntegrityError(
        "SELECT tsp.upsert_bidirectional_titan_matches(:matches)",
        {"matches": {"50001": {"private": "large-input"}}},
        ForeignKeyViolation("foreign key violation"),
    )
    session = session_mock()
    session.execute.side_effect = database_error
    match_input = [
        {
            "source_global_id": 39205,
            "source_tsp_id": 8205,
            "source_application_id": 100,
            "target_global_id": 50001,
            "target_tsp_id": 9001,
            "target_application_id": 200,
            "distance": 0.08,
        }
    ]
    store = TitanMatchesDBStore(SessionSequence([session]), LoggerFake())

    with pytest.raises(PermanentDataError) as captured:
        store.save_bidirectional_match_relationships(match_input)

    displayed_error = str(captured.value)
    assert "Titan matches were rejected" in displayed_error
    assert "fk_titan_matches_sentence_vector" in displayed_error
    assert "Key (global_id)=(50001) is not present" in displayed_error
    assert "SELECT" not in displayed_error
    assert "large-input" not in displayed_error
    assert captured.value.__suppress_context__ is True
    assert session.execute.call_count == 1


def test_relationships_are_deduplicated_and_saved_sequentially():
    first_write = session_mock(execute_value=2)
    second_write = session_mock(execute_value=1)
    store = TitanMatchesDBStore(
        SessionSequence([first_write, second_write]), LoggerFake()
    )
    relationships = [
        {
            "source_global_id": 100,
            "source_tsp_id": 10,
            "source_application_id": 1,
            "target_global_id": 200,
            "target_tsp_id": 20,
            "target_application_id": 2,
            "distance": 0.10,
        },
        {
            "source_global_id": 200,
            "source_tsp_id": 20,
            "source_application_id": 2,
            "target_global_id": 100,
            "target_tsp_id": 10,
            "target_application_id": 1,
            "distance": 0.08,
        },
        {
            "source_global_id": 100,
            "source_tsp_id": 10,
            "source_application_id": 1,
            "target_global_id": 300,
            "target_tsp_id": 30,
            "target_application_id": 3,
            "distance": 0.20,
        },
        {
            "source_global_id": 101,
            "source_tsp_id": 10,
            "source_application_id": 1,
            "target_global_id": 301,
            "target_tsp_id": 30,
            "target_application_id": 3,
            "distance": 0.15,
        },
    ]

    saved_count = store.save_bidirectional_match_relationships(relationships)

    assert saved_count == 3
    assert first_write.execute.call_args.args[1]["matches"]["200"]["distance"] == 0.08
    assert second_write.execute.call_args.args[1]["source_global_id"] == 101
