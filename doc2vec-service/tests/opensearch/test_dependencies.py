from types import SimpleNamespace

from doc2vec_completed.opensearch import dependencies as dependencies_module
from doc2vec_completed.opensearch.dependencies import Dependencies


class EngineFake:
    def __init__(self):
        self.disposed = False

    def dispose(self):
        self.disposed = True


def test_dependencies_build_and_close_all_resources(monkeypatch, dependency_config):
    config = dependency_config
    logger = object()
    engine = EngineFake()
    database = SimpleNamespace(engine=engine)
    opensearch_client = object()
    opensearch_store = object()
    aws_sqs = object()
    historical_handler = object()
    live_handler = object()
    tracer_handler = object()
    train_model_handler = object()
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

    def build_opensearch_store(received_config, client, received_logger):
        assert (received_config, client, received_logger) == (
            config,
            opensearch_client,
            logger,
        )
        return opensearch_store

    monkeypatch.setattr(
        dependencies_module,
        "OpenSearchDocumentStore",
        build_opensearch_store,
    )

    def build_sqs(**arguments):
        captured["sqs"] = arguments
        return aws_sqs

    monkeypatch.setattr(dependencies_module, "AwsSQS", build_sqs)

    def build_live_handler(*arguments):
        captured["live_handler"] = arguments
        return live_handler

    monkeypatch.setattr(dependencies_module, "LiveHandler", build_live_handler)

    def build_handler(*arguments):
        captured["handler"] = arguments
        return historical_handler

    monkeypatch.setattr(dependencies_module, "HistoricalHandler", build_handler)

    def build_tracer_handler(*arguments):
        captured["tracer_handler"] = arguments
        return tracer_handler

    monkeypatch.setattr(dependencies_module, "TracerHandler", build_tracer_handler)

    def build_train_model_handler(*arguments):
        captured["train_model_handler"] = arguments
        return train_model_handler

    monkeypatch.setattr(
        dependencies_module,
        "TrainModelHandler",
        build_train_model_handler,
    )

    dependencies = Dependencies()

    assert dependencies.logger is logger
    assert dependencies.database is database
    assert dependencies.session_factory is session_factory
    assert dependencies.opensearch_store is opensearch_store
    assert dependencies.aws_sqs is aws_sqs
    assert dependencies.historical_handler is historical_handler
    assert dependencies.live_handler is live_handler
    assert dependencies.tracer_handler is tracer_handler
    assert dependencies.train_model_handler is train_model_handler
    assert captured["database"] == {
        "pg_db_user": "database-user",
        "pg_db_password": "database-password",
        "pg_db_host": "database-host",
        "pg_db_database": "database-name",
        "pg_db_port": 5432,
        "pool_size": 20,
        "max_overflow": 10,
        "use_dynamic_credentials": False,
        "application_name": "doc2vec_opensearch",
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
        opensearch_store,
        aws_sqs,
        logger,
    )
    assert captured["live_handler"] == (
        config,
        session_factory,
        opensearch_store,
        aws_sqs,
        logger,
    )
    assert captured["tracer_handler"] == (
        config,
        session_factory,
        opensearch_store,
        logger,
    )
    assert captured["train_model_handler"] == (
        config,
        session_factory,
        opensearch_store,
        logger,
    )

    dependencies.close()

    assert engine.disposed is True
