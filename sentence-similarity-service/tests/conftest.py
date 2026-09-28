from datetime import UTC, datetime
from types import SimpleNamespace

import pytest


@pytest.fixture
def serviceConfig():
    return SimpleNamespace(
        databaseUrl="postgresql+psycopg://db",
        awsRegion="us-east-1",
        iamAccessRole="",
        opensearchHost="search.example.com",
        opensearchPort=443,
        opensearchService="es",
        sourceQueueUrl="https://sqs.example.com/source.fifo",
        nextQueueUrl="https://sqs.example.com/next.fifo",
        nextDocumentState="API_TRANSFORM",
        occurrenceAlias="occurrence",
        catalogAlias="catalog",
        tracerAlias="tracer",
        sourcePageSize=100,
        workerCount=2,
        matchWriteBatchSize=2,
        catalogPageSize=2,
        matchThreshold=90,
        tracerThreshold=90,
        ivfNprobes=16,
        vectorDimension=2,
        retrySeconds=0,
        databasePoolSize=20,
        databaseMaxOverflow=5,
        sqsWaitSeconds=20,
        sqsVisibilitySeconds=7_200,
    )


@pytest.fixture
def sentenceRow():
    return SimpleNamespace(
        global_id=101,
        application_id="APP-1",
        tsp_id=10,
        analysis_group="Asylee",
        sentence_key="a" * 64,
        is_current=True,
        is_non_matcher=False,
        is_tracer=False,
        is_form_language=False,
        updated_at=datetime.now(UTC),
    )
