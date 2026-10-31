from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from doc2vec_completed.opensearch import constants
from ta_db_connection.atatables import SentencesVector, TextSimilarityInput
from tests.opensearch.fakes import LoggerFake


@pytest.fixture
def dependency_config():
    """Provide the complete configuration used by the dependency test."""

    return SimpleNamespace(
        pg_db_user="database-user",
        pg_db_password="database-password",
        pg_db_host="database-host",
        pg_db_database="database-name",
        pg_db_port=5432,
        dynamic_credential_file="",
        environment="local",
        aws_region="us-east-1",
        aws_local_endpoint_url="",
        opensearch_host="search.example.com",
        opensearch_iam_access_role="",
        doc2vec_queue_url="https://sqs.example.com/doc2vec.fifo",
        aws_sqs_send_event_queue_endpoint="https://sqs.example.com/next.fifo",
    )


@pytest.fixture
def service_config(monkeypatch):
    monkeypatch.setattr(constants, "OPENSEARCH_BATCH_SIZE", 1000)
    monkeypatch.setattr(constants, "OPENSEARCH_BULK_RETRY_COUNT", 15)
    monkeypatch.setattr(constants, "WORKER_BATCH_SIZE", 30)
    monkeypatch.setattr(constants, "THREAD_WORKER_COUNT", 2)
    monkeypatch.setattr(constants, "TRAIN_MODEL_WORKER_COUNT", 2)
    monkeypatch.setattr(constants, "RETRY_SECONDS", 1)
    monkeypatch.setattr(constants, "VECTOR_DIMENSION", 2)
    monkeypatch.setattr(constants, "TITAN_EMBEDDING_TYPE", 2)
    monkeypatch.setattr(constants, "ASYLEE_CATALOG_ALIAS", "catalog-asylee")
    monkeypatch.setattr(constants, "REFUGEE_CATALOG_ALIAS", "catalog-refugee")
    monkeypatch.setattr(constants, "TRAINING_ALIAS", "training")
    monkeypatch.setattr(constants, "TRACER_ALIAS", "tracer")
    monkeypatch.setattr(constants, "SENTENCE_OCCURRENCE_ALIAS", "occurrence")
    return SimpleNamespace(
        aws_sqs_send_event_queue_endpoint=(
            "https://sqs.us-east-1.amazonaws.com/1/next.fifo"
        ),
        doc2vec_queue_url="https://sqs.us-east-1.amazonaws.com/1/source.fifo",
    )


@pytest.fixture
def custom_logger():
    return LoggerFake()


@pytest.fixture
def sentence_record():
    now = datetime.now(UTC)
    document = TextSimilarityInput(
        tsp_id=10,
        a_number=123456789,
        application_id=1001,
        content="The applicant moved to Boston.",
        source_type="affidavit",
        section_name="affidavit",
        analysis_group="Asylee",
        is_current=True,
    )
    return SentencesVector(
        global_id=1,
        a_number=123456789,
        tsp_id=10,
        tsp=document,
        section_name="sentence-row-section",
        sent_id_local=1,
        sentence_content="The applicant moved to Boston.",
        word_count=5,
        sentence_key="a" * 64,
        source_type="affidavit",
        analysis_group="sentence-row-group",
        embedding_list=[],
        is_non_matcher=False,
        is_tracer=False,
        is_form_language=False,
        is_current=False,
        last_modified=now,
    )

