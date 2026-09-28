from types import SimpleNamespace

from sentence_similarity import main


class ReturnValue:
    def __init__(self, value):
        self.value = value

    def __call__(self, *args, **kwargs):
        return self.value


class ClientCreator:
    def __init__(self, value):
        self.value = value

    def create_client(self):
        return self.value


def test_main_builds_worker_runs_listener_and_closes(monkeypatch):
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
    monkeypatch.setattr(main, "OpenSearchClient", ReturnValue(ClientCreator(object())))
    monkeypatch.setattr(main, "OpenSearchStore", ReturnValue(object()))
    monkeypatch.setattr(main, "SentenceSimilarityHandler", ReturnValue(object()))
    monkeypatch.setattr(main, "SqsListener", ReturnValue(listener))

    main.run()

    assert listener.ran and database.closed
