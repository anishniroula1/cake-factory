import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def readInteger(name: str, default: int, minimum: int = 1) -> int:
    """Read a whole number and reject values that are too small."""

    try:
        value = int(os.getenv(name, str(default)).strip())
    except ValueError as error:
        raise ValueError(f"{name} must be a whole number") from error
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


@dataclass(frozen=True)
class Config:
    databaseUrl: str
    awsRegion: str
    iamAccessRole: str
    opensearchHost: str
    opensearchPort: int
    opensearchService: str
    sourceQueueUrl: str
    nextQueueUrl: str
    nextDocumentState: str
    occurrenceAlias: str
    catalogAlias: str
    trainingAlias: str
    tracerAlias: str
    databaseBatchSize: int
    opensearchBatchSize: int
    workerBatchSize: int
    workerCount: int
    historicalWorkerCount: int
    retrySeconds: int
    vectorDimension: int
    titanEmbeddingType: int
    databasePoolSize: int
    databaseMaxOverflow: int
    sqsWaitSeconds: int
    sqsVisibilitySeconds: int

    @classmethod
    def fromEnvironment(cls):
        """Build configuration from environment values."""

        config = cls(
            databaseUrl=os.getenv("DATABASE_URL", "").strip(),
            awsRegion=os.getenv("AWS_REGION", "us-east-1").strip(),
            iamAccessRole=os.getenv("IAM_ACCESS_ROLE", "").strip(),
            opensearchHost=os.getenv("OPENSEARCH_HOST", "").strip(),
            opensearchPort=readInteger("OPENSEARCH_PORT", 443),
            opensearchService=os.getenv("OPENSEARCH_SERVICE", "es").strip(),
            sourceQueueUrl=os.getenv("DOC2VEC_QUEUE_URL", "").strip(),
            nextQueueUrl=os.getenv("SENTENCE_SIMILARITY_QUEUE_URL", "").strip(),
            nextDocumentState=os.getenv(
                "DOC2VEC_COMPLETE_STATE", "SIMILARITY"
            ).strip(),
            occurrenceAlias=os.getenv(
                "OPENSEARCH_OCCURRENCE_ALIAS", "sentence_occurrences"
            ).strip(),
            catalogAlias=os.getenv(
                "OPENSEARCH_CATALOG_ALIAS", "sentence_semantic_catalog"
            ).strip(),
            trainingAlias=os.getenv(
                "OPENSEARCH_TRAINING_ALIAS", "sentence_semantic_training"
            ).strip(),
            tracerAlias=os.getenv(
                "OPENSEARCH_TRACER_ALIAS", "tracer_form_language_catalog"
            ).strip(),
            databaseBatchSize=readInteger("DATABASE_BATCH_SIZE", 1_000),
            opensearchBatchSize=readInteger("OPENSEARCH_BATCH_SIZE", 1_000),
            workerBatchSize=readInteger("WORKER_BATCH_SIZE", 30),
            workerCount=readInteger("WORKER_COUNT", 16),
            historicalWorkerCount=readInteger(
                "HISTORICAL_WORKER_COUNT", 100
            ),
            retrySeconds=readInteger("RETRY_SECONDS", 5),
            vectorDimension=readInteger("VECTOR_DIMENSION", 512),
            titanEmbeddingType=readInteger("TITAN_EMBEDDING_TYPE", 2),
            databasePoolSize=readInteger("DATABASE_POOL_SIZE", 20),
            databaseMaxOverflow=readInteger(
                "DATABASE_MAX_OVERFLOW", 10, minimum=0
            ),
            sqsWaitSeconds=readInteger("SQS_WAIT_SECONDS", 20),
            sqsVisibilitySeconds=readInteger("SQS_VISIBILITY_SECONDS", 7_200),
        )
        config.validate()
        return config

    def validate(self):
        """Stop startup when a required value or safe limit is missing."""

        if not self.databaseUrl:
            raise ValueError("DATABASE_URL is required")
        if not self.opensearchHost:
            raise ValueError("OPENSEARCH_HOST is required")
        if not self.sourceQueueUrl:
            raise ValueError("DOC2VEC_QUEUE_URL is required")
        if self.workerCount > 16:
            raise ValueError("WORKER_COUNT cannot be greater than 16")
        if self.historicalWorkerCount > 100:
            raise ValueError(
                "HISTORICAL_WORKER_COUNT cannot be greater than 100"
            )
        if self.vectorDimension != 512:
            raise ValueError("VECTOR_DIMENSION must be 512 for Titan embeddings")
        if not 1 <= self.sqsWaitSeconds <= 20:
            raise ValueError("SQS_WAIT_SECONDS must be between 1 and 20")
