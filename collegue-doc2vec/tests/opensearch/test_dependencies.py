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
    opensearch_helper = object()
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
        def __init__(self, **arguments):
            captured["opensearch_client"] = arguments

        def create_client(self):
            return opensearch_client

    monkeypatch.setattr(
        dependencies_module,
        "OpenSearchClient",
        OpenSearchClientFake,
    )

    def build_opensearch_helper(client, received_logger):
        captured["opensearch_helper"] = (client, received_logger)
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
    dependencies.close()

