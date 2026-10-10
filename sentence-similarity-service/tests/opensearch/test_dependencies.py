from types import SimpleNamespace

from similarity_completed.opensearch import dependencies as dependencies_module
from similarity_completed.opensearch.dependencies import Dependencies
from tests.opensearch.fakes import LoggerFake


class EngineFake:
    def __init__(self):
        self.closed = False

    def dispose(self):
        self.closed = True


def test_dependencies_build_resources_and_close_pool(monkeypatch, service_config):
    database = SimpleNamespace(engine=EngineFake())
    sqs_values = {}

    monkeypatch.setattr(dependencies_module.Config, "from_environment", lambda: service_config)
    monkeypatch.setattr(dependencies_module, "sessionmaker", lambda **values: "session-factory")
    monkeypatch.setattr(
        dependencies_module, "OpenSearchClient", lambda config: SimpleNamespace(create_client=lambda: "client")
    )
    monkeypatch.setattr(dependencies_module, "AwsSQS", lambda **values: sqs_values.update(values) or "sqs")
    monkeypatch.setattr(dependencies_module, "SentenceSimilarityHandler", lambda *values: "handler")

    dependencies = Dependencies(LoggerFake(), database)
    dependencies.close()

    assert (
        dependencies.session_factory,
        dependencies.opensearch_client,
        dependencies.aws_sqs,
        sqs_values["queue_url"],
        database.engine.closed,
    ) == ("session-factory", "client", "sqs", "source.fifo", True)
