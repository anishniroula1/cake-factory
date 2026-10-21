from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from tests.opensearch.fakes import DependenciesFake, HandlerFake, LoggerFake, SqsFake
from transform_completed.opensearch import constants
from transform_completed.opensearch import dependencies as dependencies_module
from transform_completed.opensearch.config import Config, read_boolean, read_integer
from transform_completed.opensearch.dependencies import Dependencies
from transform_completed.opensearch.errors import OperationFailedError
from transform_completed.opensearch.main import run_opensearch_flow
from transform_completed.opensearch.retry import retry_operation
from transform_completed.opensearch.sqs_processor import SqsProcessor


def test_config_reads_environment(monkeypatch):
    values = {
        "PG_DB_USER": "postgres",
        "PG_DB_PASSWORD": "postgres",
        "PG_DB_HOST": "localhost",
        "PG_DB_DATABASE": "postgres",
        "PG_DB_PORT": "5432",
        "AWS_RECEIVE_SQS_QUEUE_ENDPOINT": "http://localhost/asylee-transform.fifo",
        "AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT": "http://localhost/next.fifo",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_ENV": "local",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    config = Config.from_environment()

    assert (config.pg_db_port, config.sentence_match_limit) == (5432, 3000)
    assert constants.KAFKA_TOPIC == "rfs.ata.data_migration.api.events"


def test_boolean_and_validation_reject_bad_values(monkeypatch, service_config):
    monkeypatch.setenv("BAD_TOGGLE", "maybe")
    with pytest.raises(ValueError, match="true or false"):
        read_boolean("BAD_TOGGLE", False)

    monkeypatch.setenv("BAD_INTEGER", "not-a-number")
    with pytest.raises(ValueError, match="whole number"):
        read_integer("BAD_INTEGER", 3000)

    with pytest.raises(ValueError, match="Missing required"):
        replace(service_config, receive_sqs_endpoint="").validate()
    with pytest.raises(ValueError, match="VAULT_YAML_PATH"):
        replace(service_config, environment="dev", dynamic_credential_file="").validate()
    with pytest.raises(ValueError, match="Kafka SSL certificate"):
        replace(service_config, kafka_security_protocol="SSL").validate()


def test_retry_reports_success_and_final_failure():
    attempts = []
    logger = LoggerFake()

    def temporary_failure():
        attempts.append(1)
        if len(attempts) == 1:
            raise OperationalError("statement", {}, RuntimeError("temporary"))
        return "done"

    assert (
        retry_operation("temporary operation", temporary_failure, wait_seconds=0, max_attempts=2, custom_logger=logger)
        == "done"
    )
    with pytest.raises(OperationFailedError):
        retry_operation(
            "failed operation",
            Mock(side_effect=OperationalError("statement", {}, RuntimeError("down"))),
            wait_seconds=0,
            max_attempts=2,
            custom_logger=logger,
        )


def test_sqs_send_failure_keeps_source_message(service_config):
    message = {"tsp_id": 10, "MessageId": "one-local", "ReceiptHandle": "receipt-local"}
    source = SqsFake([[message], []])
    sent = SqsFake(send_status="FAILED")
    processor = SqsProcessor(DependenciesFake(service_config, source, sent, HandlerFake(), LoggerFake()))

    assert processor._process_message(message) is False
    assert (len(sent.sent), source.deleted) == (1, [])


def test_main_closes_dependencies(monkeypatch):
    dependencies = Mock()
    dependencies.logger = LoggerFake()
    monkeypatch.setattr("transform_completed.opensearch.main.SqsProcessor.run", Mock())

    run_opensearch_flow(dependencies=dependencies)

    dependencies.close.assert_called_once()


def test_dependency_kafka_config_adds_ssl_files(service_config):
    dependency = object.__new__(Dependencies)
    dependency.config = replace(
        service_config,
        kafka_security_protocol="SSL",
        kafka_server_ca_cert="ca.pem",
        ssl_cert_file="client.pem",
        ssl_key_file="client.key",
    )

    values = dependency._kafka_config()

    assert values["ssl.ca.location"] == "ca.pem"


def test_dependencies_build_the_complete_worker(monkeypatch, service_config):
    source_sqs = Mock()
    dlq_sqs = Mock()
    sent_sqs = Mock()
    sqs_constructor = Mock(side_effect=[source_sqs, dlq_sqs, sent_sqs])
    producer_constructor = Mock(return_value=Mock())
    monkeypatch.setattr(dependencies_module, "AwsSQS", sqs_constructor)
    monkeypatch.setattr(dependencies_module, "Producer", producer_constructor)
    database = SimpleNamespace(engine=create_engine("sqlite://"))

    dependencies = Dependencies(LoggerFake(), database, service_config)

    assert (dependencies.aws_sqs, dependencies.dlq_sqs, dependencies.sent_sqs) == (source_sqs, dlq_sqs, sent_sqs)
    assert sqs_constructor.call_args_list[1].kwargs["max_number_of_messages"] == 10
    dependencies.close()


def test_database_connection_uses_local_or_dynamic_credentials(monkeypatch, service_config):
    database_constructor = Mock(return_value=Mock())
    monkeypatch.setattr(dependencies_module, "DatabaseConnection", database_constructor)
    dependencies = object.__new__(Dependencies)

    dependencies.config = service_config
    dependencies._create_database_connection()
    dependencies.config = replace(service_config, environment="dev", dynamic_credential_file="vault.yaml")
    dependencies._create_database_connection()

    assert database_constructor.call_args_list[0].kwargs["use_dynamic_credentials"] is False
    assert database_constructor.call_args_list[1].kwargs["dynamic_credentials_file"] == "vault.yaml"
