from dataclasses import replace
from unittest.mock import Mock

import pytest
from sqlalchemy.exc import OperationalError

from api_transform_service.opensearch.config import Config, read_boolean
from api_transform_service.opensearch.dependencies import Dependencies
from api_transform_service.opensearch.errors import OperationFailedError
from api_transform_service.opensearch.main import run_opensearch_flow
from api_transform_service.opensearch.retry import retry_operation
from api_transform_service.opensearch.sqs_processor import SqsProcessor
from tests.opensearch.fakes import DependenciesFake, HandlerFake, LoggerFake, SqsFake


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
        "ANALYSIS_GROUP": "asylee",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    config = Config.from_environment()

    assert (config.analysis_group, config.pg_db_port, config.kafka_topic) == (
        "Asylee",
        5432,
        "rfs.ata.data_migration.api.events",
    )


def test_boolean_and_validation_reject_bad_values(monkeypatch, service_config):
    monkeypatch.setenv("BAD_TOGGLE", "maybe")
    with pytest.raises(ValueError, match="true or false"):
        read_boolean("BAD_TOGGLE", False)

    with pytest.raises(ValueError, match="Missing required"):
        replace(service_config, receive_sqs_endpoint="").validate()


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
    monkeypatch.setattr("api_transform_service.opensearch.main.SqsProcessor.run", Mock())

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
