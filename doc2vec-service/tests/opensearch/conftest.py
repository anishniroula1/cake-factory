from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from doc2vec_completed.opensearch.models import SentencesVector
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
    return SentencesVector(
        global_id=1,
        application_id="APP-1",
        a_number="A-1",
        tsp_id=10,
        section_name="affidavit",
        sent_id_local=1,
        sentence_content="The applicant moved to Boston.",
        sentence_key="a" * 64,
        source_type="affidavit",
        analysis_group="Asylee",
        is_non_matcher=False,
        is_tracer=False,
        is_form_language=False,
        is_current=True,
        created_at=now,
        updated_at=now,
    )
