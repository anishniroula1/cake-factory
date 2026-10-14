import json
from unittest.mock import Mock

import pytest

from api_transform_service.opensearch.handlers.api_transform_handler import ApiTransformHandler
from api_transform_service.opensearch.payload_builder import PayloadBuilder
from api_transform_service.opensearch.sqs_processor import SqsProcessor
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


def test_one_sqs_message_runs_complete_api_transform_flow(service_config):
    message = {"tsp_id": 10, "reason": "Historical", "MessageId": "one-local", "ReceiptHandle": "receipt-one-local"}
    source = SqsFake([[message]])
    sent = SqsFake()
    handler = HandlerFake()
    processor = SqsProcessor(DependenciesFake(service_config, source, sent, handler, LoggerFake()))

    completed = processor._process_message(message)

    assert (completed, handler.calls, handler.historical_values, len(source.deleted), len(sent.sent)) == (
        True,
        [10],
        [True],
        1,
        1,
    )
    assert json.loads(sent.sent[0][1]) == {"source": "P2", "tsp_id": "10", "sent_by": "api-transform"}


def test_historical_reason_is_added_to_kafka_event(service_config):
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

    handler.process(10, historical=True)

    assert kafka.events[0]["after"]["historical"] is True


def test_run_keeps_reading_one_message_at_a_time(service_config):
    message = {"tsp_id": 10, "MessageId": "one-local", "ReceiptHandle": "receipt-one-local"}
    source = SqsFake()
    source.listen_sqs_message = Mock(side_effect=[[message], KeyboardInterrupt])
    handler = HandlerFake()
    processor = SqsProcessor(DependenciesFake(service_config, source, SqsFake(), handler, LoggerFake()))

    with pytest.raises(KeyboardInterrupt):
        processor.run()

    assert handler.calls == [10]


def test_dlq_list_processes_every_message_separately(service_config):
    messages = [
        {"tsp_id": 10, "MessageId": "one-local", "ReceiptHandle": "receipt-one-local"},
        {"tsp_id": 11, "MessageId": "two-local", "ReceiptHandle": "receipt-two-local"},
    ]
    source = SqsFake()
    source.listen_sqs_message = Mock(side_effect=[[], KeyboardInterrupt])
    dlq = SqsFake([messages])
    handler = HandlerFake()
    processor = SqsProcessor(DependenciesFake(service_config, source, SqsFake(), handler, LoggerFake(), dlq=dlq))

    with pytest.raises(KeyboardInterrupt):
        processor.run()

    assert (handler.calls, len(dlq.deleted), source.deleted) == ([10, 11], 2, [])


def test_failed_transform_keeps_source_messages(service_config):
    message = {"tsp_id": 10, "MessageId": "one-local", "ReceiptHandle": "receipt-local"}
    source = SqsFake([[message], []])
    processor = SqsProcessor(DependenciesFake(service_config, source, SqsFake(), HandlerFake(fail=True), LoggerFake()))

    assert processor._process_message(message) is False
    assert source.deleted == []


def test_invalid_message_is_left_for_sqs_retry(service_config):
    invalid = {"MessageId": "invalid-local", "ReceiptHandle": "invalid-receipt-local"}
    source = SqsFake([[invalid]])
    processor = SqsProcessor(DependenciesFake(service_config, source, SqsFake(), HandlerFake(), LoggerFake()))

    assert processor._process_message(invalid) is False
    assert source.deleted == []


def test_delete_failure_returns_false(service_config):
    message = {"tsp_id": 10, "MessageId": "one-local", "ReceiptHandle": "receipt-one-local"}
    source = SqsFake(delete_status="FAILED")
    processor = SqsProcessor(DependenciesFake(service_config, source, SqsFake(), HandlerFake(), LoggerFake()))

    assert processor._process_message(message) is False


def test_handler_builds_same_payload_without_ner(service_config):
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

    result = handler.process(10)
    payload = kafka.events[0]["after"]["payload"]

    assert result == {"source_message_count": 1, "application_count": 1, "kafka_event_count": 1}
    assert set(payload) == {"a_records", "documents", "applicant_details", "sentences", "public_templates"}
    assert payload["sentences"][0]["matches"] == [{"global_id": 20, "distance": 0.1}]
