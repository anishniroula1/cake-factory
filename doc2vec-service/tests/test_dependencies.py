from types import SimpleNamespace

from doc2vec_service import dependencies
from tests.fakes import LoggerFake


class ReturnValue:
    def __init__(self, value):
        self.value = value

    def __call__(self, *args, **kwargs):
        return self.value


class Creator:
    def __init__(self, value):
        self.value = value

    def create_client(self):
        return self.value


def configValues():
    return SimpleNamespace(
        pgDbUser="user",
        pgDbPassword="pass",
        pgDbHost="database",
        pgDbDatabase="name",
        pgDbPort=5433,
        databasePoolSize=20,
        databaseMaxOverflow=10,
        sourceQueueUrl="source-queue",
        nextQueueUrl="next-queue",
        awsRegion="us-east-1",
        sqsWaitSeconds=20,
        sqsVisibilitySeconds=7_200,
        retrySeconds=5,
    )


def test_dependencies_build_everything_needed_by_the_processor(monkeypatch):
    config = configValues()
    engine = SimpleNamespace(dispose=lambda: None)
    database = SimpleNamespace(engine=engine)
    opensearch = object()
    awsSqs = object()
    handler = object()

    monkeypatch.setattr(
        dependencies.Config,
        "fromEnvironment",
        lambda: config,
    )
    monkeypatch.setattr(
        dependencies,
        "sessionmaker",
        ReturnValue("session-factory"),
    )
    monkeypatch.setattr(
        dependencies,
        "OpenSearchClient",
        ReturnValue(Creator("client")),
    )
    monkeypatch.setattr(
        dependencies,
        "OpenSearchHelper",
        ReturnValue(opensearch),
    )
    monkeypatch.setattr(dependencies, "AwsSQS", ReturnValue(awsSqs))
    monkeypatch.setattr(
        dependencies,
        "HistoricalHandler",
        ReturnValue(handler),
    )

    values = dependencies.Dependencies(LoggerFake(), database)

    assert values.database is database
    assert values.sessionFactory == "session-factory"
    assert values.opensearch is opensearch
    assert values.awsSqs is awsSqs
    assert values.historicalHandler is handler


def test_dependencies_create_and_close_database_connection(monkeypatch):
    logger = LoggerFake()
    captured = {}

    def databaseConnection(**values):
        captured.update(values)
        return SimpleNamespace(
            engine=SimpleNamespace(dispose=lambda: captured.update(closed=True))
        )

    monkeypatch.setattr(dependencies, "DatabaseConnection", databaseConnection)
    values = object.__new__(dependencies.Dependencies)
    values.logger = logger
    values.config = configValues()
    values.database = values._createDatabaseConnection()

    assert captured["pg_db_user"] == "user"
    assert captured["pg_db_port"] == 5433
    assert captured["application_name"] == "doc2vec_historical"

    values.close()
    assert captured["closed"]
