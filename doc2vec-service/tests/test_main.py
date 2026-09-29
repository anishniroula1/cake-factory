from types import SimpleNamespace

from doc2vec_service import main


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


def test_main_builds_handlers_runs_listener_and_closes(monkeypatch):
    config = SimpleNamespace(awsRegion="us-east-1")

    class AwsSession:
        def client(self, name, region_name):
            return "sqs-client"

    class Database:
        sessionFactory = object()

        def close(self):
            self.closed = True

    class Listener:
        def run(self):
            self.ran = True

    database = Database()
    listener = Listener()

    def settings():
        return config

    monkeypatch.setattr(main.Config, "fromEnvironment", settings)
    monkeypatch.setattr(main.boto3, "Session", ReturnValue(AwsSession()))
    monkeypatch.setattr(main, "Database", ReturnValue(database))
    monkeypatch.setattr(main, "OpenSearchClient", ReturnValue(Creator(object())))
    monkeypatch.setattr(main, "OpenSearchHelper", ReturnValue(object()))
    monkeypatch.setattr(main, "TrainHandler", ReturnValue(object()))
    monkeypatch.setattr(main, "TracerHandler", ReturnValue(object()))
    monkeypatch.setattr(main, "HistoricalHandler", ReturnValue(object()))
    monkeypatch.setattr(main, "LiveHandler", ReturnValue(object()))
    monkeypatch.setattr(main, "SqsListener", ReturnValue(listener))

    main.run()

    assert listener.ran and database.closed
