from datetime import UTC, datetime

import pytest

from similarity_completed.opensearch.config import Config
from ta_db_connection.atatables import SentencesVector, TextSimilarityInput


@pytest.fixture
def service_config():
    return Config(
        pg_db_user="user",
        pg_db_password="password",
        pg_db_host="database",
        pg_db_database="sentence",
        pg_db_port=5432,
        database_pool_size=30,
        database_max_overflow=10,
        dynamic_credential_file="",
        aws_region="us-east-1",
        aws_local_endpoint_url="",
        opensearch_iam_access_role="",
        opensearch_host="search.example.com",
        opensearch_port=443,
        opensearch_service="es",
        occurrence_alias="occurrence",
        asylee_catalog_alias="catalog-asylee",
        refugee_catalog_alias="catalog-refugee",
        tracer_alias="tracer",
        vector_dimension=2,
        match_threshold=76,
        tracer_threshold=70,
        ivf_nprobes=64,
        receive_sqs_endpoint="source.fifo",
        sent_sqs_endpoint="next.fifo",
        sqs_wait_seconds=10,
        sqs_visibility_seconds=7_200,
        source_sentence_batch_size=30,
        thread_worker_count=1,
        titan_match_write_batch_size=500,
        opensearch_terms_batch_size=1_000,
        database_classification_batch_size=1_000,
        retry_seconds=0,
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
