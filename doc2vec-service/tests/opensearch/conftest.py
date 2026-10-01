from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from doc2vec_completed.opensearch.models import SentencesVector, TextSimilarityInput
from tests.opensearch.fakes import LoggerFake


@pytest.fixture
def service_config():
    return SimpleNamespace(
        opensearch_batch_size=1_000,
        worker_batch_size=30,
        thread_worker_count=2,
        retry_seconds=1,
        vector_dimension=2,
        titan_embedding_type=2,
        catalog_alias="catalog",
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
        application_id="APP-1",
    )
    return SentencesVector(
        global_id=1,
        a_number=123456789,
        tsp_id=10,
        tsp=document,
        section_name="affidavit",
        sent_id_local=1,
        sentence_content="The applicant moved to Boston.",
        word_count=5,
        sentence_key="a" * 64,
        source_type="affidavit",
        analysis_group="Asylee",
        embedding_list=[],
        is_non_matcher=False,
        closest_non_matcher=None,
        closest_nn_score=None,
        is_tracer=False,
        is_form_language=False,
        is_current=True,
        last_modified=now,
        in_scope=True,
    )
