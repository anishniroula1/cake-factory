from datetime import UTC, datetime

import pytest

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.config import Config
from ta_db_connection.atatables import SentencesVector, TextSimilarityInput


@pytest.fixture
def service_config(monkeypatch):
    # Keep unit-test vectors and worker pools small while production code reads
    # every fixed value from the shared constants module.
    monkeypatch.setattr(constants, "VECTOR_DIMENSION", 2)
    monkeypatch.setattr(constants, "THREAD_WORKER_COUNT", 1)
    monkeypatch.setattr(constants, "RETRY_SECONDS", 0)
    monkeypatch.setattr(constants, "SENTENCE_OCCURRENCE_ALIAS", "occurrence")
    monkeypatch.setattr(constants, "ASYLEE_CATALOG_ALIAS", "catalog-asylee")
    monkeypatch.setattr(constants, "REFUGEE_CATALOG_ALIAS", "catalog-refugee")
    monkeypatch.setattr(constants, "TRACER_ALIAS", "tracer")
    return Config(
        pg_db_user="user",
        pg_db_password="password",
        pg_db_host="database",
        pg_db_database="sentence",
        pg_db_port=5432,
        dynamic_credential_file="",
        environment="local",
        aws_region="us-east-1",
        aws_local_endpoint_url="",
        opensearch_iam_access_role="",
        opensearch_host="search.example.com",
        receive_sqs_endpoint="source.fifo",
        sent_sqs_endpoint="next.fifo",
    )


@pytest.fixture
def sentence_row():
    document = TextSimilarityInput(
        tsp_id=10,
        application_id=1,
        section_name="affidavit",
        analysis_group="Asylee",
        is_current=True,
        document_state="SIMILARITY",
    )
    return SentencesVector(
        global_id=100,
        tsp_id=10,
        a_number=123,
        section_name="affidavit",
        sent_id_local=1,
        sentence_content="The applicant lived in Ethiopia.",
        word_count=5,
        source_type="affidavit",
        analysis_group="Asylee",
        is_current=True,
        embedding_list=[],
        is_non_matcher=False,
        is_form_language=False,
        is_tracer=False,
        last_modified=datetime.now(UTC),
        in_scope=True,
        sentence_key="a" * 64,
        tsp=document,
    )
