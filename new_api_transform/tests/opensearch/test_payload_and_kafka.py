import json

import pytest

from tests.opensearch.fakes import KafkaProducerFake, LoggerFake, TransmissionStoreFake
from transform_completed.opensearch.errors import KafkaPublishError
from transform_completed.opensearch.kafka_processor import KafkaProcessor
from transform_completed.opensearch.payload_builder import PayloadBuilder


def application_record(group="Asylee"):
    return {
        "application_id": 100,
        "a_number": 900001,
        "analysis_group": group,
        "receipt_number": "ABC123",
        "similar_case_ids": [800002],
        "similar_case_application_ids": [200],
    }


def test_refugee_payload_normalizes_receipt_and_has_no_template(service_config):
    event = PayloadBuilder(service_config).build_application_events(application_record("Refugee"), None, [], [], None)[
        0
    ]

    assert event["after"]["payload"]["a_records"]["receipt_number"] == "ABC-00000123"
    assert "public_templates" not in event["after"]["payload"]


def test_large_payload_splits_sentences(tiny_payload_config):
    sentences = [{"global_id": value, "sentence": "x" * 250} for value in range(6)]
    events = PayloadBuilder(tiny_payload_config).build_application_events(application_record(), {}, [], sentences, None)

    assert len(events) > 1
    assert (events[0]["op"], events[1]["op"]) == ("c", "u")


def test_kafka_publishes_string_payload_and_saves_acknowledgement(service_config):
    event = PayloadBuilder(service_config).build_application_events(application_record(), {}, [], [], None)[0]
    producer = KafkaProducerFake()
    store = TransmissionStoreFake()
    processor = KafkaProcessor(service_config, producer, store, LoggerFake())

    assert processor.publish_application_events([event]) == 1
    published = json.loads(producer.published[0][2])
    assert isinstance(published["after"]["payload"], str)
    assert len(store.deliveries) == 1


@pytest.mark.parametrize(
    "producer", [KafkaProducerFake(delivery_error=RuntimeError("rejected")), KafkaProducerFake(pending=1)]
)
def test_kafka_failure_does_not_save_delivery(service_config, producer):
    event = PayloadBuilder(service_config).build_application_events(application_record(), {}, [], [], None)[0]
    store = TransmissionStoreFake()

    with pytest.raises(KafkaPublishError):
        KafkaProcessor(service_config, producer, store, LoggerFake()).publish_application_events([event])

    assert store.deliveries == []
