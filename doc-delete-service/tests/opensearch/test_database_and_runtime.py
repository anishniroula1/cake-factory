from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch

from doc_delete_completed.opensearch.config import Config
from doc_delete_completed.opensearch.db_store.embeddings_db_store import EmbeddingsDBStore
from doc_delete_completed.opensearch.db_store.sentence_vector_db_store import SentenceVectorDBStore
from doc_delete_completed.opensearch.db_store.titan_matches_db_store import TitanMatchesDBStore
from doc_delete_completed.opensearch.dependencies import Dependencies
from doc_delete_completed.opensearch.main import run_opensearch_flow
from tests.opensearch.fakes import LoggerFake


def service_config(**changes) -> Config:
    values = Config(
        pg_db_user="database-user",
        pg_db_password="database-password",
        pg_db_host="database-host",
        pg_db_database="database-name",
        pg_db_port=5432,
        dynamic_credential_file="",
        environment="local",
        aws_region="us-east-1",
        aws_local_endpoint_url="",
        opensearch_iam_access_role="",
        opensearch_host="search.example.com",
        receive_sqs_endpoint="https://sqs.example/source-local.fifo",
        sent_sqs_endpoint="https://sqs.example/next-local.fifo",
        database_delete_enabled=True,
    )
    return replace(values, **changes)


def session_with_results(*results):
    session = Mock()
    session.__enter__ = Mock(return_value=session)
    session.__exit__ = Mock(return_value=False)
    session.execute.side_effect = results
    return session


def result(**methods):
    value = Mock()
    for name, returned in methods.items():
        getattr(value, name).return_value = returned
    return value


def test_database_store_loads_context_and_deletes_owned_rows():
    context_session = session_with_results(
        result(
            all=[
                SimpleNamespace(global_id=1, sentence_key="key-a", analysis_group="Asylee"),
                SimpleNamespace(global_id=2, sentence_key=None, analysis_group="Asylee"),
            ]
        )
    )
    function_row = SimpleNamespace(
        source_global_id_count=2,
        deleted_match_bucket_count=3,
        updated_reverse_bucket_count=4,
        deleted_empty_bucket_count=1,
    )
    match_session = session_with_results(result(one=function_row))
    embedding_session = session_with_results(SimpleNamespace(rowcount=2))
    sentence_delete_session = session_with_results(SimpleNamespace(rowcount=2))

    sentence_store = SentenceVectorDBStore(lambda: context_session)
    context = sentence_store.load_document_sentence_context(10)
    deleted_matches = TitanMatchesDBStore(lambda: match_session).delete_bidirectional_document_matches(10)
    deleted_embeddings = EmbeddingsDBStore(lambda: embedding_session).delete_sentence_embeddings(context.global_ids)
    deleted_sentences = SentenceVectorDBStore(lambda: sentence_delete_session).delete_document_sentence_records(10)

    assert (context.global_ids, context.sentence_keys) == ([1, 2], ["key-a"])
    assert (deleted_matches.deleted_match_bucket_count, deleted_matches.updated_reverse_bucket_count) == (3, 4)
    assert (deleted_embeddings, deleted_sentences) == (2, 2)
    match_session.commit.assert_called_once()


def test_database_store_returns_empty_context_when_document_has_no_sentences():
    store = SentenceVectorDBStore(lambda: session_with_results(result(all=[])))

    context = store.load_document_sentence_context(10)

    assert (context.analysis_group, context.global_ids, context.sentence_keys) == (None, [], [])


def test_embeddings_store_skips_database_when_there_are_no_global_ids():
    session_factory = Mock()

    assert EmbeddingsDBStore(session_factory).delete_sentence_embeddings([]) == 0
    session_factory.assert_not_called()


def test_dependencies_build_resources_and_close_pool():
    database = SimpleNamespace(engine=Mock())
    sqs = Mock()
    with (
        patch("doc_delete_completed.opensearch.dependencies.OpenSearchClient.create_client", return_value=Mock()),
        patch("doc_delete_completed.opensearch.dependencies.AwsSQS", return_value=sqs),
    ):
        dependencies = Dependencies(LoggerFake(), database, service_config())

    assert dependencies.document_delete_handler.aws_sqs is sqs
    dependencies.close()
    database.engine.dispose.assert_called_once()


def test_dependencies_pass_dynamic_database_configuration():
    config = service_config(environment="dev", dynamic_credential_file="vault.yaml")
    dependencies = Dependencies.__new__(Dependencies)
    dependencies.config = config

    with patch("doc_delete_completed.opensearch.dependencies.DatabaseConnection") as database_connection:
        dependencies._create_database_connection()

    assert database_connection.call_args.kwargs["use_dynamic_credentials"] is True


def test_main_runs_processor_and_always_closes_dependencies():
    dependencies = SimpleNamespace(logger=LoggerFake(), close=Mock())

    with patch("doc_delete_completed.opensearch.main.SqsProcessor") as processor:
        run_opensearch_flow(dependencies=dependencies)

    processor.return_value.run.assert_called_once()
    dependencies.close.assert_called_once()
    assert dependencies.logger.generic_message is None
