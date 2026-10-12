from datetime import UTC, datetime, timedelta

import pytest

from api_transform_service.opensearch.contracts import KafkaDelivery
from api_transform_service.opensearch.db_store.applicant_db_store import ApplicantDBStore
from api_transform_service.opensearch.db_store.application_db_store import ApplicationDBStore
from api_transform_service.opensearch.db_store.content_db_store import ContentDBStore
from api_transform_service.opensearch.db_store.transmission_db_store import TransmissionDBStore
from tests.opensearch.fakes import SessionFactoryFake, SessionFake


def test_application_store_reads_unique_application_values():
    sessions = SessionFactoryFake(
        [
            SessionFake([{"application_id": 100, "a_number": 900001}]),
            SessionFake(
                [{"application_id": 100, "a_number": 900001, "analysis_group": "Asylee", "receipt_number": "ABC123"}]
            ),
        ]
    )
    store = ApplicationDBStore(sessions)
    reference = store.load_application_reference(10)
    records = store.load_application_records([reference])

    assert (reference.application_id, records[100]["receipt_number"]) == (100, "ABC123")


def test_applicant_store_builds_ocr_flags():
    now = datetime.now(UTC)
    row = {
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
    details = ApplicantDBStore(SessionFactoryFake([SessionFake([row])])).load_applicant_details([100], False)

    assert details[100]["ocr_name_fault"] is True
    assert "first_name_ocr" not in details[100]


def test_content_store_converts_titan_bucket_result_and_match_update():
    now = datetime.now(UTC)
    sentence_row = {
        "application_id": 100,
        "global_id": 1,
        "matches": [{"global_id": "20", "distance": 0.123456}],
        "matches_count": 1,
        "matches_last_modified": now,
        "sentences_last_modified": now - timedelta(hours=1),
    }
    document_row = {"application_id": 100, "tsp_id": 10, "match_count": 1}
    template_row = {"application_id": 100, "a_number": 900001, "url": "one"}
    sessions = SessionFactoryFake(
        [SessionFake([sentence_row]), SessionFake([document_row]), SessionFake([template_row])]
    )
    store = ContentDBStore(sessions, 3000)

    sentences = store.load_sentences([100], True)
    documents = store.load_documents([100])
    templates = store.load_public_templates([100])

    assert sentences[100][0] == {"global_id": 1, "matches": [{"global_id": 20, "distance": 0.1234}], "op": "u"}
    assert (documents[100][0]["match_count"], templates[100]["url"]) == (1, "one")


def test_transmission_store_commits_confirmed_delivery():
    session = SessionFake()
    store = TransmissionDBStore(SessionFactoryFake([session]), "local")
    delivery = KafkaDelivery(100, 900001, "Asylee", "{}", datetime.now(UTC), 1, 2)

    store.save_deliveries([delivery], 60)

    assert (session.commits, len(session.executions)) == (1, 3)


@pytest.mark.parametrize(
    ("value", "expected"), [("000000", False), ("000100", True), (None, False), (["0000", "0010"], True)]
)
def test_ocr_calculation(value, expected):
    assert ApplicantDBStore.calculate_ocr(value) is expected
