import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from tests.opensearch.fakes import (
    ApplicantStoreFake,
    ApplicationStoreFake,
    ContentStoreFake,
    DependenciesFake,
    HandlerFake,
    KafkaProcessorFake,
    LoggerFake,
    SqsFake,
    TransmissionStoreFake,
)
from transform_completed.opensearch import config as config_module
from transform_completed.opensearch import constants
from transform_completed.opensearch import dependencies as dependencies_module
from transform_completed.opensearch.config import Config, read_boolean, read_integer
from transform_completed.opensearch.dependencies import Dependencies
from transform_completed.opensearch.errors import OperationFailedError, PermanentDataError
from transform_completed.opensearch.handlers.api_transform_handler import ApiTransformHandler
from transform_completed.opensearch.main import run_opensearch_flow
from transform_completed.opensearch.payload_builder import PayloadBuilder
from transform_completed.opensearch.retry import retry_operation
from transform_completed.opensearch.sqs_processor import SqsProcessor


def test_one_sqs_message_runs_complete_api_transform_flow(service_config):
    message = {"tsp_id": 10, "MessageId": "one-local", "ReceiptHandle": "receipt-one-local"}
    source = SqsFake([[message]])
    sent = SqsFake()
    handler = HandlerFake()
    processor = SqsProcessor(DependenciesFake(service_config, source, sent, handler, LoggerFake()))

    completed = processor._process_document_message(message)

    assert (completed, handler.calls, len(source.deleted), len(sent.sent)) == (True, [10], 1, 1)
    assert json.loads(sent.sent[0][1]) == {"source": "P2", "tsp_id": "10", "sent_by": "api-transform"}


def test_backfill_queue_skips_next_event_and_deletes_source_message(service_config):
    message = {"tsp_id": 10, "MessageId": "backfill-local", "ReceiptHandle": "receipt-backfill-local"}
    source = SqsFake([[message]])
    sent = SqsFake()
    config = replace(service_config, receive_sqs_endpoint="http://localhost/api-transform-backfill.fifo")
    processor = SqsProcessor(DependenciesFake(config, source, sent, HandlerFake(), LoggerFake()))

    completed = processor._process_document_message(message)

    assert (completed, len(source.deleted), len(sent.sent)) == (True, 1, 0)


def test_handler_builds_titan_payload_without_ner(service_config):
    kafka = KafkaProcessorFake()
    handler = ApiTransformHandler(
        service_config,
        ApplicationStoreFake(),
        ApplicantStoreFake(),
        ContentStoreFake(),
        TransmissionStoreFake(),
        PayloadBuilder(service_config),
        kafka,
        LoggerFake(),
    )

    result = handler.process_document(10)
    event = kafka.events[0]

    assert result == {"source_message_count": 1, "application_count": 1, "kafka_event_count": 1}
    assert event["after"]["embedding_model"] == "TITAN"
    assert set(event["after"]["payload"]) == {
        "a_records",
        "documents",
        "applicant_details",
        "sentences",
        "public_templates",
    }


def test_source_and_dlq_messages_are_processed_separately(service_config):
    source_message = {"tsp_id": 10, "MessageId": "one-local", "ReceiptHandle": "receipt-one-local"}
    source = SqsFake()
    source.listen_sqs_message = Mock(side_effect=[[source_message], KeyboardInterrupt])
    source_handler = HandlerFake()

    with pytest.raises(KeyboardInterrupt):
        SqsProcessor(DependenciesFake(service_config, source, SqsFake(), source_handler, LoggerFake())).run()

    dlq_messages = [
        {"tsp_id": 11, "MessageId": "two-local", "ReceiptHandle": "receipt-two-local"},
        {"tsp_id": 12, "MessageId": "three-local", "ReceiptHandle": "receipt-three-local"},
    ]
    empty_source = SqsFake()
    empty_source.listen_sqs_message = Mock(side_effect=[[], KeyboardInterrupt])
    dlq = SqsFake([dlq_messages])
    dlq_handler = HandlerFake()

    with pytest.raises(KeyboardInterrupt):
        SqsProcessor(
            DependenciesFake(service_config, empty_source, SqsFake(), dlq_handler, LoggerFake(), dlq=dlq)
        ).run()

    assert (source_handler.calls, dlq_handler.calls, len(dlq.deleted)) == ([10], [11, 12], 2)


def test_failed_messages_are_not_deleted(service_config):
    message = {"tsp_id": 10, "MessageId": "one-local", "ReceiptHandle": "receipt-local"}

    failed_source = SqsFake()
    failed_transform = SqsProcessor(
        DependenciesFake(service_config, failed_source, SqsFake(), HandlerFake(fail=True), LoggerFake())
    )
    assert failed_transform._process_document_message(message) is False
    assert failed_source.deleted == []

    invalid_source = SqsFake()
    invalid_processor = SqsProcessor(
        DependenciesFake(service_config, invalid_source, SqsFake(), HandlerFake(), LoggerFake())
    )
    assert invalid_processor._process_document_message({"MessageId": "invalid-local"}) is False

    delete_failure = SqsProcessor(
        DependenciesFake(service_config, SqsFake(delete_status="FAILED"), SqsFake(), HandlerFake(), LoggerFake())
    )
    assert delete_failure._process_document_message(message) is False

    send_source = SqsFake()
    send_failure = SqsProcessor(
        DependenciesFake(service_config, send_source, SqsFake(send_status="FAILED"), HandlerFake(), LoggerFake())
    )
    assert send_failure._process_document_message(message) is False
    assert send_source.deleted == []


def test_config_reads_vault_and_rejects_invalid_values(monkeypatch, service_config):
    vault_values = {
        "pgUser": "vault-user",
        "pgPassword": "vault-password",
        "pgHost": "vault-database",
        "pgDatabase": "vault-name",
        "pgPort": "5432",
    }
    vault_secrets = Mock()
    vault_secrets.get.side_effect = lambda name, default=None: vault_values.get(name, default)
    monkeypatch.setattr(config_module, "VaultSecrets", Mock(return_value=vault_secrets))
    for name, value in {
        "AWS_RECEIVE_SQS_QUEUE_ENDPOINT": "http://localhost/asylee-transform.fifo",
        "AWS_SQS_SEND_EVENT_QUEUE_ENDPOINT": "http://localhost/next.fifo",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_ENV": "local",
    }.items():
        monkeypatch.setenv(name, value)

    config = Config.from_environment()

    assert (config.pg_db_user, config.pg_db_host, config.pg_db_port) == ("vault-user", "vault-database", 5432)
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


def test_retry_succeeds_then_reports_final_failure():
    attempts = []
    logger = LoggerFake()

    def temporary_failure():
        attempts.append(1)
        if len(attempts) == 1:
            raise OperationalError("statement", {}, RuntimeError("temporary"))
        return "done"

    assert retry_operation(
        "temporary operation", temporary_failure, wait_seconds=0, max_attempts=2, custom_logger=logger
    ) == "done"
    assert "statement" not in str(logger.messages)
    with pytest.raises(OperationFailedError):
        retry_operation(
            "failed operation",
            Mock(side_effect=OperationalError("statement", {}, RuntimeError("down"))),
            wait_seconds=0,
            max_attempts=2,
            custom_logger=logger,
        )


def test_failure_record_retries_without_hiding_the_transform_error(monkeypatch, service_config):
    application_store = ApplicationStoreFake()
    application_store.load_application_reference = Mock(
        side_effect=PermanentDataError("Application data is invalid")
    )
    transmission_store = TransmissionStoreFake()
    transmission_store.save_transform_error = Mock(
        side_effect=OperationalError("sensitive statement", {}, RuntimeError("database unavailable"))
    )
    logger = LoggerFake()
    handler = ApiTransformHandler(
        service_config,
        application_store,
        ApplicantStoreFake(),
        ContentStoreFake(),
        transmission_store,
        PayloadBuilder(service_config),
        KafkaProcessorFake(),
        logger,
    )
    monkeypatch.setattr("transform_completed.opensearch.retry.time.sleep", lambda _seconds: None)

    with pytest.raises(PermanentDataError, match="Application data is invalid"):
        handler.process_document(10)

    assert transmission_store.save_transform_error.call_count == constants.MAX_ATTEMPTS
    assert "sensitive statement" not in str(logger.messages)


def test_runtime_builds_and_closes_dependencies(monkeypatch, service_config):
    dependency = object.__new__(Dependencies)
    dependency.config = replace(
        service_config,
        kafka_security_protocol="SSL",
        kafka_server_ca_cert="ca.pem",
        ssl_cert_file="client.pem",
        ssl_key_file="client.key",
    )
    assert dependency._build_kafka_producer_config()["ssl.ca.location"] == "ca.pem"

    sqs_constructor = Mock(side_effect=[Mock(), Mock(), Mock()])
    monkeypatch.setattr(dependencies_module, "AwsSQS", sqs_constructor)
    monkeypatch.setattr(dependencies_module, "Producer", Mock(return_value=Mock()))
    built_dependencies = Dependencies(
        LoggerFake(), SimpleNamespace(engine=create_engine("sqlite://")), service_config
    )
    assert sqs_constructor.call_args_list[1].kwargs["max_number_of_messages"] == 10
    built_dependencies.close()

    database_constructor = Mock(return_value=Mock())
    monkeypatch.setattr(dependencies_module, "DatabaseConnection", database_constructor)
    dependency.config = service_config
    dependency._create_database_connection()
    dependency.config = replace(service_config, environment="dev", dynamic_credential_file="vault.yaml")
    dependency._create_database_connection()
    assert database_constructor.call_args_list[0].kwargs["use_dynamic_credentials"] is False
    assert database_constructor.call_args_list[1].kwargs["dynamic_credentials_file"] == "vault.yaml"

    main_dependencies = Mock()
    main_dependencies.logger = LoggerFake()
    monkeypatch.setattr("transform_completed.opensearch.main.SqsProcessor.run", Mock())
    run_opensearch_flow(dependencies=main_dependencies)
    main_dependencies.close.assert_called_once()
