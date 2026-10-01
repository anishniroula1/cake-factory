from types import SimpleNamespace

from doc2vec_completed.opensearch import dependencies as dependencies_module
from doc2vec_completed.opensearch.dependencies import Dependencies


class EngineFake:
    def __init__(self):
        self.disposed = False

    def dispose(self):
        self.disposed = True


def dependency_config():
    return SimpleNamespace(
        pg_db_user="database-user",
        pg_db_password="database-password",
        pg_db_host="database-host",
        pg_db_database="database-name",
        pg_db_port=5432,
        database_pool_size=20,
        database_max_overflow=10,
        dynamic_credential_file="",
        aws_region="us-east-1",
        aws_endpoint_url="",
        opensearch_host="search.example.com",
        opensearch_port=443,
        opensearch_service="es",
        opensearch_iam_access_role="",
        opensearch_batch_size=1_000,
        occurrence_alias="sentence_occurrences",
        catalog_alias="sentence_semantic_catalog",
        vector_dimension=512,
        doc2vec_queue_url="https://sqs.example.com/doc2vec.fifo",
        sqs_wait_seconds=10,
        sqs_visibility_seconds=7_200,
        retry_seconds=5,
        sentence_similarity_backfill_queue_url=(
            "https://sqs.example.com/similarity.fifo"
        ),
        worker_batch_size=30,
        thread_worker_count=100,
        titan_embedding_type=2,
    )


def test_dependencies_build_and_close_all_resources(monkeypatch):
    config = dependency_config()
    logger = object()
    engine = EngineFake()
    database = SimpleNamespace(engine=engine)
    opensearch_client = object()
    opensearch_helper = object()
    aws_sqs = object()
    historical_handler = object()
    session_factory = object()
    captured = {}

    monkeypatch.setattr(
        dependencies_module,
        "Config",
        SimpleNamespace(from_environment=lambda: config),
    )
    monkeypatch.setattr(dependencies_module, "CustomLogger", lambda name: logger)

    def build_database(**arguments):
        captured["database"] = arguments
        return database

    monkeypatch.setattr(dependencies_module, "DatabaseConnection", build_database)

    def build_session_factory(**arguments):
        captured["session_factory"] = arguments
        return session_factory

    monkeypatch.setattr(
        dependencies_module,
        "sessionmaker",
        build_session_factory,
    )

    class OpenSearchClientFake:
        def __init__(self, received_config):
            assert received_config is config

        def create_client(self):
            return opensearch_client

    monkeypatch.setattr(
        dependencies_module,
        "OpenSearchClient",
        OpenSearchClientFake,
    )

    def build_opensearch_helper(received_config, client, received_logger):
        assert (received_config, client, received_logger) == (
            config,
            opensearch_client,
            logger,
        )
        return opensearch_helper

    monkeypatch.setattr(
        dependencies_module,
        "OpenSearchHelper",
        build_opensearch_helper,
    )

    def build_sqs(**arguments):
        captured["sqs"] = arguments
        return aws_sqs

    monkeypatch.setattr(dependencies_module, "AwsSQS", build_sqs)

    def build_handler(*arguments):
        captured["handler"] = arguments
        return historical_handler

    monkeypatch.setattr(dependencies_module, "HistoricalHandler", build_handler)

    dependencies = Dependencies()

    assert dependencies.logger is logger
    assert dependencies.database is database
    assert dependencies.session_factory is session_factory
    assert dependencies.opensearch is opensearch_helper
    assert dependencies.aws_sqs is aws_sqs
    assert dependencies.historical_handler is historical_handler
    assert captured["database"] == {
        "pg_db_user": "database-user",
        "pg_db_password": "database-password",
        "pg_db_host": "database-host",
        "pg_db_database": "database-name",
        "pg_db_port": 5432,
        "pool_size": 20,
        "max_overflow": 10,
        "dynamic_credentials_file": None,
        "application_name": "doc2vec_historical",
        "logger": logger,
    }
    assert captured["session_factory"] == {
        "bind": engine,
        "autoflush": False,
        "expire_on_commit": False,
    }
    assert captured["sqs"]["queue_url"].endswith("doc2vec.fifo")
    assert captured["handler"] == (
        config,
        session_factory,
        opensearch_helper,
        aws_sqs,
        logger,
    )

    dependencies.close()

    assert engine.disposed is True
