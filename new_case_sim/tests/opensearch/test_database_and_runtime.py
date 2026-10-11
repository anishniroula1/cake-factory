from dataclasses import replace

import pytest
from sqlalchemy.exc import OperationalError

from case_similarity_service.opensearch import config as config_module
from case_similarity_service.opensearch import dependencies as dependencies_module
from case_similarity_service.opensearch import main as main_module
from case_similarity_service.opensearch.contracts import KafkaDelivery
from case_similarity_service.opensearch.db_store.case_similarity_db_store import CaseSimilarityDBStore
from case_similarity_service.opensearch.db_store.kafka_event_db_store import KafkaEventDBStore
from case_similarity_service.opensearch.dependencies import Dependencies
from case_similarity_service.opensearch.errors import OperationFailedError, PermanentDataError
from case_similarity_service.opensearch.retry import retry_operation
from tests.opensearch.fakes import (
    AwsSqsFake,
    DatabaseConnectionFake,
    KafkaProducerFake,
    LoggerFake,
    MainDependenciesFake,
    ProcessorFake,
    ResultFake,
    SessionFactoryFake,
    SessionFake,
    VaultSecretsFake,
)


class RetryAction:
    def __init__(self) -> None:
        self.calls = 0

    def run(self):
        self.calls += 1
        if self.calls == 1:
            raise OperationalError("statement", {}, RuntimeError("busy"))
        return "finished"


class AlwaysFailAction:
    def run(self):
        raise OperationalError("statement", {}, RuntimeError("busy"))


def test_database_store_loads_document_and_refreshes_case_summaries(service_config):
    document_session = SessionFake(
        ResultFake([{"tsp_id": 10, "application_id": 100, "a_number": 900001, "analysis_group": "Asylee"}])
    )
    summary_session = SessionFake(
        ResultFake(
            [
                {
                    "application_id": 100,
                    "similar_application_ids": [200, 300],
                    "similar_case_ids": [300],
                    "similar_case_count": 1,
                    "tah_case_ids": [],
                    "tah_case_count": 0,
                    "is_tah": False,
                }
            ]
        )
    )
    store = CaseSimilarityDBStore(
        service_config,
        SessionFactoryFake([document_session, summary_session]),
        LoggerFake(),
    )

    context = store.load_document_context(10)
    summaries = store.refresh_case_summaries(100, "Asylee")

    assert {
        "context": (context.tsp_id, context.application_id, context.a_number, context.analysis_group),
        "similar_application_ids": summaries[0].similar_application_ids,
        "similar_case_ids": summaries[0].similar_case_ids,
        "summary_commits": summary_session.commits,
    } == {
        "context": (10, 100, 900001, "Asylee"),
        "similar_application_ids": [200, 300],
        "similar_case_ids": [300],
        "summary_commits": 1,
    }


def test_kafka_database_store_reads_and_saves_delivery():
    load_session = SessionFake(
        ResultFake(
            [
                {
                    "receipt_number": "ABC123",
                    "application_id": 100,
                    "a_number": 900001,
                    "analysis_group": "Asylee",
                    "is_tah": True,
                }
            ]
        )
    )
    save_session = SessionFake()
    store = KafkaEventDBStore(SessionFactoryFake([load_session, save_session]))

    event = store.load_changed_tah_events([100], "local")[0]
    store.save_kafka_delivery(event, KafkaDelivery("2026-10-10T12:00:00+00:00", 1, 20), "local")

    assert (event.receipt_number, event.application_id, len(save_session.executions), save_session.commits) == (
        "ABC123",
        100,
        1,
        1,
    )


def test_database_stores_return_empty_results_and_report_missing_documents(service_config):
    empty_document_session = SessionFake(ResultFake())
    case_store = CaseSimilarityDBStore(
        service_config,
        SessionFactoryFake([empty_document_session]),
        LoggerFake(),
    )
    kafka_store = KafkaEventDBStore(SessionFactoryFake([]))

    with pytest.raises(PermanentDataError, match="tsp_id=999"):
        case_store.load_document_context(999)

    assert kafka_store.load_changed_tah_events([], "local") == []


def test_retry_repeats_only_the_failed_operation():
    action = RetryAction()

    result = retry_operation("database write", action.run, wait_seconds=0, custom_logger=LoggerFake())

    assert (result, action.calls) == ("finished", 2)


def test_runtime_starts_processor_and_closes_dependencies(monkeypatch):
    dependencies = MainDependenciesFake()
    ProcessorFake.run_count = 0
    monkeypatch.setattr(main_module, "SqsProcessor", ProcessorFake)

    main_module.run_opensearch_flow(dependencies=dependencies)

    assert (ProcessorFake.run_count, dependencies.closed) == (1, 1)


def test_config_validation_and_kafka_values(service_config):
    service_config.validate()
    dependencies = object.__new__(Dependencies)
    dependencies.config = service_config

    assert dependencies._kafka_config()["security.protocol"] == "PLAINTEXT"
    with pytest.raises(ValueError, match="KAFKA_ENV"):
        replace(service_config, kafka_environment="").validate()


def test_config_loads_local_environment_values(monkeypatch):
    environment_values = {
        "ENVIRONMENT": "local",
        "AWS_RECEIVE_SQS_QUEUE_ENDPOINT": "http://aws-local:4566/case-similarity-local.fifo",
        "AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT": "http://aws-local:4566/next-local.fifo",
        "AWS_LOCAL_ENDPOINT_URL": "http://aws-local:4566",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_ENV": "local",
        "SEND_TAH_KAFKA_EVENTS": "true",
        "APPLICATION_URL": "https://ui.example/case/",
    }
    monkeypatch.setattr(config_module, "VaultSecrets", VaultSecretsFake)
    for name, value in environment_values.items():
        monkeypatch.setenv(name, value)

    config = config_module.Config.from_environment()

    assert {
        "database": (config.pg_db_host, config.pg_db_port),
        "receive_queue": config.receive_sqs_endpoint,
        "kafka": (config.kafka_bootstrap_servers, config.kafka_security_protocol),
        "application_url": config.application_url,
        "send_tah": config.send_tah_kafka_events,
    } == {
        "database": ("database-local", 5432),
        "receive_queue": "http://aws-local:4566/case-similarity-local.fifo",
        "kafka": ("localhost:9092", "PLAINTEXT"),
        "application_url": "https://ui.example/case/",
        "send_tah": True,
    }


@pytest.mark.parametrize(
    ("config_changes", "error_message"),
    [
        ({"environment": "dev", "dynamic_credential_file": ""}, "VAULT_YAML_PATH"),
        (
            {
                "kafka_security_protocol": "SSL",
                "kafka_server_ca_cert": "",
                "ssl_cert_file": "",
                "ssl_key_file": "",
            },
            "Kafka SSL certificate values",
        ),
    ],
)
def test_config_rejects_missing_deployment_credentials(service_config, config_changes, error_message):
    with pytest.raises(ValueError, match=error_message):
        replace(service_config, **config_changes).validate()


def test_dependencies_build_local_services_and_close_database(monkeypatch, service_config):
    database = DatabaseConnectionFake()
    monkeypatch.setattr(dependencies_module, "AwsSQS", AwsSqsFake)
    monkeypatch.setattr(dependencies_module, "Producer", KafkaProducerFake)

    dependencies = Dependencies(database_connection=database, config=service_config)
    dependencies.close()

    assert {
        "queue_url": dependencies.aws_sqs.init_values["queue_url"],
        "kafka_servers": dependencies.kafka_producer.config["bootstrap.servers"],
        "handler_created": dependencies.case_similarity_handler is not None,
        "database_closed": database.engine.disposed,
    } == {
        "queue_url": service_config.receive_sqs_endpoint,
        "kafka_servers": "kafka-local:9092",
        "handler_created": True,
        "database_closed": True,
    }


def test_dependencies_use_local_or_dynamic_database_credentials(monkeypatch, service_config):
    monkeypatch.setattr(dependencies_module, "DatabaseConnection", DatabaseConnectionFake)
    dependencies = object.__new__(Dependencies)

    dependencies.config = service_config
    local_database = dependencies._create_database_connection()

    dependencies.config = replace(
        service_config,
        environment="dev",
        dynamic_credential_file="/vault/database.yml",
        kafka_security_protocol="SSL",
        kafka_server_ca_cert="ca.pem",
        ssl_cert_file="client.pem",
        ssl_key_file="client-key.pem",
    )
    dynamic_database = dependencies._create_database_connection()
    kafka_config = dependencies._kafka_config()

    assert {
        "local_credentials": local_database.arguments["use_dynamic_credentials"],
        "dynamic_file": dynamic_database.arguments["dynamic_credentials_file"],
        "kafka_ca": kafka_config["ssl.ca.location"],
    } == {
        "local_credentials": False,
        "dynamic_file": "/vault/database.yml",
        "kafka_ca": "ca.pem",
    }


def test_boolean_environment_value_must_be_true_or_false(monkeypatch):
    monkeypatch.setenv("TEST_TOGGLE", "sometimes")

    with pytest.raises(ValueError, match="TEST_TOGGLE"):
        config_module.read_boolean("TEST_TOGGLE", False)


def test_retry_raises_after_the_last_attempt():
    with pytest.raises(OperationFailedError):
        retry_operation(
            "database write", AlwaysFailAction().run, wait_seconds=0, max_attempts=1, custom_logger=LoggerFake()
        )
