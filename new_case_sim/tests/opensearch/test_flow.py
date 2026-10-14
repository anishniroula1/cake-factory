import json
from dataclasses import replace
from unittest.mock import Mock

import pytest

from case_similarity_completed.opensearch.errors import KafkaPublishError, QueueSendError
from case_similarity_completed.opensearch.handlers.case_similarity_handler import CaseSimilarityHandler
from case_similarity_completed.opensearch.kafka_processor import KafkaProcessor
from case_similarity_completed.opensearch.sqs_processor import SqsProcessor
from tests.opensearch.fakes import (
    AwsSqsFake,
    CaseSimilarityDBStoreFake,
    DependenciesFake,
    HandlerFake,
    KafkaEventDBStoreFake,
    KafkaMessageFake,
    KafkaProducerFake,
    LoggerFake,
)


def test_one_live_message_runs_the_complete_case_similarity_flow(service_config):
    message = {"tsp_id": 10, "MessageId": "case-local", "ReceiptHandle": "receipt-local"}
    database_store = CaseSimilarityDBStoreFake()
    kafka_event_store = KafkaEventDBStoreFake()
    kafka_producer = KafkaProducerFake()
    kafka_processor = KafkaProcessor(service_config, kafka_producer, kafka_event_store, LoggerFake())
    aws_sqs = AwsSqsFake([message])
    handler = CaseSimilarityHandler(service_config, database_store, kafka_processor, aws_sqs, LoggerFake())
    processor = SqsProcessor(DependenciesFake(aws_sqs, handler, LoggerFake()))

    processed_message_count = processor.run_once()
    kafka_payload = json.loads(kafka_producer.published[0][2])

    assert {
        "processed_message_count": processed_message_count,
        "refreshed_applications": database_store.refreshed,
        "next_message": aws_sqs.sent[0]["message_body"],
        "deleted_messages": aws_sqs.deleted,
        "kafka_key": kafka_producer.published[0][1],
        "kafka_payload": kafka_payload,
        "saved_delivery_count": len(kafka_event_store.deliveries),
    } == {
        "processed_message_count": 1,
        "refreshed_applications": [(100, "Asylee")],
        "next_message": {"source": "P2", "tsp_id": "10", "sent_by": "case-similarity"},
        "deleted_messages": [message],
        "kafka_key": "ABC123",
        "kafka_payload": {
            "is_tah": True,
            "application_url": "https://ui.example/case/100",
            "primary_a_number": "900001",
            "analysis_group": "Asylee",
        },
        "saved_delivery_count": 1,
    }


def test_failed_message_is_not_deleted():
    message = {"tsp_id": 10, "ReceiptHandle": "receipt-local"}
    aws_sqs = AwsSqsFake([message])
    processor = SqsProcessor(DependenciesFake(aws_sqs, HandlerFake(failure=True), LoggerFake()))

    processor.run_once()

    assert aws_sqs.deleted == []


def test_kafka_processor_skips_historical_flow(service_config):
    producer = KafkaProducerFake()
    processor = KafkaProcessor(
        replace(service_config, send_tah_kafka_events=False), producer, KafkaEventDBStoreFake(), LoggerFake()
    )

    event_count = processor.publish_changed_tah_events([100])

    assert (event_count, producer.published) == (0, [])


def test_live_kafka_flow_handles_no_changes_and_missing_producer(service_config):
    no_change_processor = KafkaProcessor(
        service_config, KafkaProducerFake(), KafkaEventDBStoreFake(events=[]), LoggerFake()
    )

    assert no_change_processor.publish_changed_tah_events([100]) == 0

    with pytest.raises(KafkaPublishError, match="producer is unavailable"):
        KafkaProcessor(service_config, None, KafkaEventDBStoreFake(), LoggerFake()).publish_changed_tah_events([100])


@pytest.mark.parametrize(
    "producer_values",
    [
        {"produce_error": BufferError("producer queue is full")},
        {"delivery_error": RuntimeError("broker rejected the message")},
        {"pending_message_count": 1},
        {"acknowledge": False},
    ],
)
def test_kafka_delivery_failure_does_not_save_a_lookup_row(service_config, producer_values):
    event_store = KafkaEventDBStoreFake()
    processor = KafkaProcessor(service_config, KafkaProducerFake(**producer_values), event_store, LoggerFake())

    with pytest.raises(KafkaPublishError):
        processor._publish_event(event_store.events[0])

    assert event_store.deliveries == []


def test_kafka_uses_current_time_when_broker_timestamp_is_missing():
    timestamp = KafkaProcessor._delivery_timestamp(KafkaMessageFake(0))

    assert timestamp.endswith("+00:00")


def test_handler_reports_when_next_queue_send_fails(service_config):
    handler = CaseSimilarityHandler(
        service_config,
        CaseSimilarityDBStoreFake(),
        KafkaProcessor(service_config, KafkaProducerFake(), KafkaEventDBStoreFake(), LoggerFake()),
        AwsSqsFake(send_result=False),
        LoggerFake(),
    )

    with pytest.raises(QueueSendError):
        handler._send_next_message(10)


def test_sqs_processor_reports_missing_id_and_delete_failure():
    missing_id_message = {"MessageId": "missing-id-local", "ReceiptHandle": "missing-local"}
    valid_message = {"tsp_id": "10", "MessageId": "case-local", "ReceiptHandle": "receipt-local"}
    aws_sqs = AwsSqsFake([missing_id_message, valid_message], delete_result=False)
    handler = HandlerFake()
    logger = LoggerFake()
    processor = SqsProcessor(DependenciesFake(aws_sqs, handler, logger))

    message_count = processor.run_once()
    errors = [message for level, message in logger.messages if level == "error"]

    assert {
        "message_count": message_count,
        "processed_tsp_ids": handler.calls,
        "delete_attempts": aws_sqs.deleted,
        "error_count": len(errors),
        "generic_message": logger.generic_message,
    } == {
        "message_count": 2,
        "processed_tsp_ids": ["10"],
        "delete_attempts": [valid_message],
        "error_count": 2,
        "generic_message": None,
    }


def test_sqs_run_keeps_listening():
    processor = SqsProcessor(DependenciesFake(AwsSqsFake(), HandlerFake(), LoggerFake()))
    run_once = Mock(side_effect=KeyboardInterrupt)
    processor.run_once = run_once

    with pytest.raises(KeyboardInterrupt):
        processor.run()

    assert run_once.call_count == 1
