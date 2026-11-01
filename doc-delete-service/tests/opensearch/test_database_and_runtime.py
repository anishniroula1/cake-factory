from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch

from doc_delete_job.opensearch import constants
from doc_delete_job.opensearch.config import Config
from doc_delete_job.opensearch.db_store.embeddings_db_store import EmbeddingsDBStore
from doc_delete_job.opensearch.db_store.sentence_vector_db_store import SentenceVectorDBStore
from doc_delete_job.opensearch.db_store.text_similarity_input_db_store import TextSimilarityInputDBStore
from doc_delete_job.opensearch.db_store.titan_matches_db_store import TitanMatchesDBStore
from doc_delete_job.opensearch.dependencies import Dependencies
from doc_delete_job.opensearch.main import run_document_delete_job
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
        send_sqs_endpoint="https://sqs.example/next-local.fifo",
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
                SimpleNamespace(global_id=1),
                SimpleNamespace(global_id=2),
            ]
        )
    )
    function_row = SimpleNamespace(
        deleted_match_bucket_count=3,
        updated_reverse_bucket_count=4,
    )
    match_session = session_with_results(result(one=function_row))
    embedding_session = session_with_results(SimpleNamespace(rowcount=2))
    sentence_delete_session = session_with_results(SimpleNamespace(rowcount=2))

    sentence_store = SentenceVectorDBStore(lambda: context_session)
    context = sentence_store.load_document_sentence_context(10)
    deleted_matches = TitanMatchesDBStore(lambda: match_session).delete_bidirectional_document_matches(10)
    deleted_embeddings = EmbeddingsDBStore(lambda: embedding_session).delete_sentence_embeddings(context.global_ids)
    deleted_sentences = SentenceVectorDBStore(lambda: sentence_delete_session).delete_document_sentence_records(10)

    assert context.global_ids == [1, 2]
    assert (deleted_matches.deleted_match_bucket_count, deleted_matches.updated_reverse_bucket_count) == (3, 4)
    assert (deleted_embeddings, deleted_sentences) == (2, 2)
    match_session.commit.assert_called_once()


def test_database_store_returns_empty_context_when_document_has_no_sentences():
    store = SentenceVectorDBStore(lambda: session_with_results(result(all=[])))

    context = store.load_document_sentence_context(10)

    assert context.global_ids == []


def test_text_similarity_input_store_loads_ready_documents_in_order():
    load_session = Mock()
    load_session.__enter__ = Mock(return_value=load_session)
    load_session.__exit__ = Mock(return_value=False)
    load_session.scalars.return_value = [101, 102]
    update_session = session_with_results(SimpleNamespace(rowcount=1))
    sessions = iter([load_session, update_session])
    store = TextSimilarityInputDBStore(lambda: next(sessions))

    tsp_ids = store.load_ready_for_deletion_tsp_ids(100, 200)
    updated_count = store.update_document_state(101, "DOC_DELETED")

    assert tsp_ids == [101, 102]
    assert updated_count == 1
    update_session.commit.assert_called_once()


def test_embeddings_store_skips_database_when_there_are_no_global_ids():
    session_factory = Mock()

    assert EmbeddingsDBStore(session_factory).delete_sentence_embeddings([]) == 0
    session_factory.assert_not_called()


def test_dependencies_build_resources_and_close_pool():
    database = SimpleNamespace(engine=Mock())
    sqs = Mock()
    config = service_config(opensearch_iam_access_role="arn:aws:iam::123456789012:role/opensearch")
    with (
        patch("doc_delete_job.opensearch.dependencies.OpenSearchClient") as open_search_client,
        patch("doc_delete_job.opensearch.dependencies.AwsSQS", return_value=sqs),
    ):
        dependencies = Dependencies(LoggerFake(), database, config)

    open_search_client.assert_called_once_with(
        opensearch_host=config.opensearch_host,
        opensearch_iam_access_role=config.opensearch_iam_access_role,
        aws_region=config.aws_region,
        opensearch_port=constants.OPENSEARCH_PORT,
        opensearch_service=constants.OPENSEARCH_SERVICE,
        role_session_name="docDeleteJobRole",
    )
    open_search_client.return_value.create_client.assert_called_once_with()
    assert dependencies.document_delete_handler.outbound_sqs is sqs
    assert dependencies.document_delete_job.document_delete_handler is dependencies.document_delete_handler
    dependencies.close()
    database.engine.dispose.assert_called_once()


def test_dependencies_pass_dynamic_database_configuration():
    config = service_config(environment="dev", dynamic_credential_file="vault.yaml")
    dependencies = Dependencies.__new__(Dependencies)
    dependencies.config = config

    with patch("doc_delete_job.opensearch.dependencies.DatabaseConnection") as database_connection:
        dependencies._create_database_connection()

    assert database_connection.call_args.kwargs["use_dynamic_credentials"] is True


def test_main_runs_job_and_always_closes_dependencies():
    dependencies = SimpleNamespace(logger=LoggerFake(), document_delete_job=Mock(), close=Mock())

    run_document_delete_job(dependencies=dependencies)

    dependencies.document_delete_job.run.assert_called_once()
    dependencies.close.assert_called_once()
    assert dependencies.logger.generic_message is None
