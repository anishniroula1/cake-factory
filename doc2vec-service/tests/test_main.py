from doc2vec_service import main
from tests.fakes import LoggerFake


def test_main_runs_processor_with_one_dependency_container(monkeypatch):
    class DependencyContainer:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    class Processor:
        def __init__(self, dependencies):
            self.dependencies = dependencies
            self.ran = False

        def run(self):
            self.ran = True

    dependencies = DependencyContainer()
    captured = {}

    def buildDependencies(customLogger, databaseConnection):
        captured["logger"] = customLogger
        captured["database"] = databaseConnection
        return dependencies

    def buildProcessor(values):
        processor = Processor(values)
        captured["processor"] = processor
        return processor

    monkeypatch.setattr(main, "Dependencies", buildDependencies)
    monkeypatch.setattr(main, "SqsProcessor", buildProcessor)

    logger = LoggerFake()
    database = object()
    main.run(logger, database)

    assert captured["logger"] is logger
    assert captured["database"] is database
    assert captured["processor"].dependencies is dependencies
    assert captured["processor"].ran
    assert dependencies.closed
