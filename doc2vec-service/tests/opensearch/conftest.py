from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from ta_db_connection.atables import SentencesVector, TextSimilarityInput
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
        database_pool_size=20,
        database_max_overflow=10,
        dynamic_credential_file="",
        environment="local",
        aws_region="us-east-1",
        aws_endpoint_url="",
        opensearch_host="search.example.com",
        opensearch_port=443,
        opensearch_service="es",
        opensearch_iam_access_role="",
        opensearch_batch_size=1_000,
        opensearch_bulk_retry_count=15,
        occurrence_alias="sentence_occurrences",
        asylee_catalog_alias="sentence_semantic_catalog_asylee",
        refugee_catalog_alias="sentence_semantic_catalog_refugee",
        training_alias="sentence_semantic_training",
        tracer_alias="tracer_form_language_catalog",
        vector_dimension=512,
        doc2vec_queue_url="https://sqs.example.com/doc2vec.fifo",
        sqs_wait_seconds=10,
        sqs_visibility_seconds=7_200,
        retry_seconds=5,
        sentence_similarity_backfill_queue_url=(
            "https://sqs.example.com/similarity.fifo"
        ),
        worker_batch_size=30,
        thread_worker_count=100,
        train_model_worker_count=16,
        titan_embedding_type=2,
    )


@pytest.fixture
def service_config():
    return SimpleNamespace(
        opensearch_batch_size=1_000,
        opensearch_bulk_retry_count=15,
        worker_batch_size=30,
        thread_worker_count=2,
        train_model_worker_count=2,
        retry_seconds=1,
        vector_dimension=2,
        titan_embedding_type=2,
        asylee_catalog_alias="catalog-asylee",
        refugee_catalog_alias="catalog-refugee",
        training_alias="training",
        tracer_alias="tracer",
        occurrence_alias="occurrence",
        sentence_similarity_backfill_queue_url=(
            "https://sqs.us-east-1.amazonaws.com/1/next.fifo"
        ),
        doc2vec_queue_url="https://sqs.us-east-1.amazonaws.com/1/source.fifo",
        sqs_wait_seconds=1,
        sqs_visibility_seconds=7_200,
    )


@pytest.fixture
def custom_logger():
    return LoggerFake()


@pytest.fixture
def sentence_record():
    now = datetime.now(UTC)
    document = TextSimilarityInput(
        tsp_id=10,
        application_id=1001,
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
        closest_non_matcher=None,
        closest_nn_score=None,
        is_tracer=False,
        is_form_language=False,
        is_current=False,
        last_modified=now,
        in_scope=True,
    )
