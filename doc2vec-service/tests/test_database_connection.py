import pytest

from doc2vec_service.DatabaseConnection import DatabaseConnection


def test_database_connection_builds_the_sqlalchemy_pool(
    monkeypatch,
    customLogger,
):
    engine = object()
    captured = {}

    def createEngine(url, **values):
        captured["url"] = url
        captured.update(values)
        return engine

    monkeypatch.setattr(
        "doc2vec_service.DatabaseConnection.create_engine",
        createEngine,
    )
    connection = DatabaseConnection(
        pg_db_user="user",
        pg_db_password="password",
        pg_db_host="database-host",
        pg_db_database="database-name",
        pool_size=20,
        pool_recycle_time=900,
        max_overflow=10,
        application_name="doc2vec-test",
        logger=customLogger,
    )

    assert connection.engine is engine
    assert captured["url"].drivername == "postgresql+psycopg"
    assert captured["pool_pre_ping"] is True
    assert captured["pool_recycle"] == 900
    assert captured["connect_args"]["application_name"] == "doc2vec-test"


def test_local_connection_requires_the_shared_class_for_dynamic_credentials(
    customLogger,
):
    with pytest.raises(ValueError):
        DatabaseConnection(
            pg_db_user="",
            pg_db_password="",
            pg_db_host="",
            pg_db_database="",
            use_dynamic_credentials=True,
            logger=customLogger,
        )
