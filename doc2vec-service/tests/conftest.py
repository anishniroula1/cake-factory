from datetime import UTC, datetime
from types import SimpleNamespace

import pytest


@pytest.fixture
def serviceConfig():
    return SimpleNamespace(
        databaseBatchSize=1_000,
        opensearchBatchSize=1_000,
        workerBatchSize=30,
        workerCount=2,
        retrySeconds=1,
        vectorDimension=2,
        titanEmbeddingType=2,
        trainingAlias="training",
        catalogAlias="catalog",
        occurrenceAlias="occurrence",
        tracerAlias="tracer",
        nextDocumentState="SIMILARITY",
        nextQueueUrl="https://sqs.us-east-1.amazonaws.com/1/next.fifo",
        sourceQueueUrl="https://sqs.us-east-1.amazonaws.com/1/source.fifo",
        sqsWaitSeconds=1,
        sqsVisibilitySeconds=7_200,
    )


@pytest.fixture
def sentenceRecord():
    now = datetime.now(UTC)
    return SimpleNamespace(
        global_id=1,
        application_id="APP-1",
        a_number="A-1",
        tsp_id=10,
        section_name="affidavit",
        sent_id_local=1,
        sentence_content="The applicant moved to Boston.",
        sentence_key="a" * 64,
        source_type="AFFIDAVIT",
        analysis_group="Asylee",
        is_non_matcher=False,
        is_tracer=False,
        is_form_language=False,
        created_at=now,
        updated_at=now,
    )
