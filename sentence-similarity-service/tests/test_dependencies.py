from types import SimpleNamespace

from sentence_similarity import dependencies as dependencies_module
from sentence_similarity.dependencies import Dependencies
from tests.fakes import LoggerFake


def test_dependencies_build_resources_and_close_pool(monkeypatch, service_config):
    class Engine:
        def __init__(self):
            self.closed = False

        def dispose(self):
            self.closed = True

    database = SimpleNamespace(engine=Engine())

    monkeypatch.setattr(
        dependencies_module.Config,
        "from_environment",
        lambda: service_config,
    )
    monkeypatch.setattr(
        dependencies_module,
        "sessionmaker",
        lambda **values: "session-factory",
    )
    monkeypatch.setattr(
        dependencies_module,
        "OpenSearchClient",
        lambda config: SimpleNamespace(create_client=lambda: "client"),
    )
    monkeypatch.setattr(
        dependencies_module,
        "AwsSQS",
        lambda **values: "sqs",
    )
    monkeypatch.setattr(
        dependencies_module,
        "SentenceSimilarityHandler",
        lambda *values: "handler",
    )

    dependencies = Dependencies(LoggerFake(), database)
    dependencies.close()

    assert dependencies.session_factory == "session-factory"
    assert dependencies.opensearch_client == "client"
    assert dependencies.aws_sqs == "sqs"
    assert dependencies.sentence_similarity_handler == "handler"
    assert database.engine.closed is True
