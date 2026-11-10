import json
from datetime import UTC, datetime, timedelta

import pytest

from tests.opensearch.fakes import KafkaProducerFake, LoggerFake, SessionFactoryFake, SessionFake, TransmissionStoreFake
from transform_completed.opensearch.contracts import KafkaDelivery
from transform_completed.opensearch.db_store.applicant_db_store import ApplicantDBStore
from transform_completed.opensearch.db_store.application_db_store import ApplicationDBStore
from transform_completed.opensearch.db_store.content_db_store import ContentDBStore
from transform_completed.opensearch.db_store.transmission_db_store import TransmissionDBStore
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


def test_payload_builder_handles_refugee_and_large_payload(service_config, tiny_payload_config):
    refugee_event = PayloadBuilder(service_config).build_application_events(
        application_record("Refugee"), None, [], [], None
    )[0]
    sentences = [{"global_id": value, "sentence": "x" * 250} for value in range(6)]
    split_events = PayloadBuilder(tiny_payload_config).build_application_events(
        application_record(), {}, [], sentences, None
    )

    assert refugee_event["after"]["payload"]["a_records"]["receipt_number"] == "ABC-00000123"
    assert "public_templates" not in refugee_event["after"]["payload"]
    assert len(split_events) > 1
    assert (split_events[0]["op"], split_events[1]["op"]) == ("c", "u")


def test_kafka_publish_success_and_failures(service_config):
    event = PayloadBuilder(service_config).build_application_events(application_record(), {}, [], [], None)[0]
    producer = KafkaProducerFake()
    transmission_store = TransmissionStoreFake()

    assert KafkaProcessor(service_config, producer, transmission_store, LoggerFake()).publish_application_events(
        [event]
    ) == 1
    assert isinstance(json.loads(producer.published[0][2])["after"]["payload"], str)
    assert len(transmission_store.deliveries) == 1

    for failed_producer in (KafkaProducerFake(delivery_error=RuntimeError("rejected")), KafkaProducerFake(pending=1)):
        failed_store = TransmissionStoreFake()
        with pytest.raises(KafkaPublishError):
            KafkaProcessor(service_config, failed_producer, failed_store, LoggerFake()).publish_application_events(
                [event]
            )
        assert failed_store.deliveries == []


def test_application_and_applicant_stores_build_payload_data():
    application_sessions = SessionFactoryFake(
        [
            SessionFake([{"application_id": 100, "a_number": 900001, "analysis_group": "Asylee"}]),
            SessionFake(
                [{"application_id": 100, "a_number": 900001, "analysis_group": "Asylee", "receipt_number": "ABC123"}]
            ),
        ]
    )
    application_store = ApplicationDBStore(application_sessions)
    reference = application_store.load_application_reference(10)
    application_payloads = application_store.load_application_payload_records([reference])

    now = datetime.now(UTC)
    applicant_record = {
        "application_id": 100,
        "first_name": "Ada",
        "last_modified": now,
        "source_type": "FORM",
        "first_name_ocr": "0001",
        "middle_name_ocr": None,
        "last_name_ocr": "0000",
        "dob_ocr": "0000",
        "country_of_citizenship_ocr": "0000",
        "residence_address_ocr": "0000",
        "residence_apt_ocr": None,
        "residence_city_ocr": "0000",
        "residence_state_ocr": "0000",
        "residence_zip_ocr": "0000",
        "mailing_address_ocr": "0000",
        "mailing_apt_ocr": None,
        "mailing_city_ocr": "0000",
        "mailing_state_ocr": "0000",
        "mailing_zip_ocr": "0000",
    }
    applicant_payloads = ApplicantDBStore(
        SessionFactoryFake([SessionFake([applicant_record])])
    ).load_applicant_details([100], False)

    assert (reference.application_id, application_payloads[100]["receipt_number"]) == (100, "ABC123")
    assert applicant_payloads[100]["ocr_name_fault"] is True
    assert "first_name_ocr" not in applicant_payloads[100]
    for value, expected in (("000000", False), ("000100", True), (None, False), (["0000", "0010"], True)):
        assert ApplicantDBStore.has_ocr_fault(value) is expected


def test_content_store_builds_payload_and_handles_empty_input():
    now = datetime.now(UTC)
    sentence_record = {
        "application_id": 100,
        "global_id": 1,
        "matches": [{"global_id": "20", "distance": 0.123456}],
        "matches_count": 1,
        "matches_last_modified": now,
        "sentences_last_modified": now - timedelta(hours=1),
    }
    content_store = ContentDBStore(
        SessionFactoryFake(
            [
                SessionFake([sentence_record]),
                SessionFake([{"application_id": 100, "tsp_id": 10, "match_count": 1}]),
                SessionFake([{"application_id": 100, "a_number": 900001, "url": "one"}]),
            ]
        )
    )

    sentences = content_store.load_sentence_payload_records([100], True)
    documents = content_store.load_document_payload_records([100])
    templates = content_store.load_public_template_payload_records([100])
    empty_store = ContentDBStore(SessionFactoryFake([]))

    assert sentences[100][0] == {"global_id": 1, "matches": [{"global_id": 20, "distance": 0.1234}], "op": "u"}
    assert (documents[100][0]["match_count"], templates[100]["url"]) == (1, "one")
    assert (
        empty_store.load_sentence_payload_records([], False),
        empty_store.load_document_payload_records([]),
        empty_store.load_public_template_payload_records([]),
        empty_store._has_only_match_changes(None, None),
    ) == ({}, {}, {}, False)


def test_transmission_store_saves_deliveries_errors_and_audits():
    delivery_session = SessionFake()
    delivery_store = TransmissionDBStore(SessionFactoryFake([delivery_session]), "local")
    delivery_store.save_confirmed_kafka_deliveries(
        [KafkaDelivery(100, 900001, "Asylee", "{}", datetime.now(UTC), 1, 2)], 60
    )

    error_session = SessionFake()
    error_store = TransmissionDBStore(SessionFactoryFake([error_session]), "local")
    event = {"after": {"payload": json.dumps({"sentences": [{"global_id": 1}, {"global_id": 2}]})}}
    error_store.save_confirmed_kafka_deliveries([], 60)
    error_store.save_transform_error("API Transform failed", RuntimeError("database unavailable"))
    compact = json.loads(error_store.build_kafka_audit_value(event, verbose_logging=False))
    verbose = json.loads(error_store.build_kafka_audit_value(event, verbose_logging=True))

    assert (delivery_session.commits, len(delivery_session.executions)) == (1, 3)
    assert (error_session.commits, compact["after"]["payload"]["sentences"]) == (1, 2)
    assert len(verbose["after"]["payload"]["sentences"]) == 2
