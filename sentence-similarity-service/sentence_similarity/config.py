import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def readInteger(name: str, default: int, minimum: int = 1) -> int:
    """Read one whole number from the environment."""

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
    tracerAlias: str
    sourcePageSize: int
    workerCount: int
    matchWriteBatchSize: int
    catalogPageSize: int
    matchThreshold: int
    tracerThreshold: int
    ivfNprobes: int
    vectorDimension: int
    retrySeconds: int
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
            sourceQueueUrl=os.getenv(
                "SENTENCE_SIMILARITY_QUEUE_URL", ""
            ).strip(),
            nextQueueUrl=os.getenv("API_TRANSFORM_QUEUE_URL", "").strip(),
            nextDocumentState=os.getenv(
                "SIMILARITY_COMPLETE_STATE", "API_TRANSFORM"
            ).strip(),
            occurrenceAlias=os.getenv(
                "OPENSEARCH_OCCURRENCE_ALIAS", "sentence_occurrences"
            ).strip(),
            catalogAlias=os.getenv(
                "OPENSEARCH_CATALOG_ALIAS", "sentence_semantic_catalog"
            ).strip(),
            tracerAlias=os.getenv(
                "OPENSEARCH_TRACER_ALIAS", "tracer_form_language_catalog"
            ).strip(),
            sourcePageSize=readInteger("SOURCE_PAGE_SIZE", 100),
            workerCount=readInteger("WORKER_COUNT", 16),
            matchWriteBatchSize=readInteger("MATCH_WRITE_BATCH_SIZE", 500),
            catalogPageSize=readInteger("CATALOG_PAGE_SIZE", 1_000),
            matchThreshold=readInteger("MATCH_THRESHOLD", 90),
            tracerThreshold=readInteger("TRACER_THRESHOLD", 90),
            ivfNprobes=readInteger("IVF_NPROBES", 16),
            vectorDimension=readInteger("VECTOR_DIMENSION", 512),
            retrySeconds=readInteger("RETRY_SECONDS", 5),
            databasePoolSize=readInteger("DATABASE_POOL_SIZE", 20),
            databaseMaxOverflow=readInteger(
                "DATABASE_MAX_OVERFLOW", 5, minimum=0
            ),
            sqsWaitSeconds=readInteger("SQS_WAIT_SECONDS", 20),
            sqsVisibilitySeconds=readInteger("SQS_VISIBILITY_SECONDS", 7_200),
        )
        config.validate()
        return config

    def validate(self):
        """Stop startup when a required value or safe limit is invalid."""

        if not self.databaseUrl:
            raise ValueError("DATABASE_URL is required")
        if not self.opensearchHost:
            raise ValueError("OPENSEARCH_HOST is required")
        if not self.sourceQueueUrl:
            raise ValueError("SENTENCE_SIMILARITY_QUEUE_URL is required")
        if not self.nextQueueUrl:
            raise ValueError("API_TRANSFORM_QUEUE_URL is required")
        if self.workerCount > 20:
            raise ValueError("WORKER_COUNT cannot be greater than 20")
        if self.vectorDimension != 512:
            raise ValueError("VECTOR_DIMENSION must be 512")
        if not 1 <= self.matchThreshold <= 100:
            raise ValueError("MATCH_THRESHOLD must be between 1 and 100")
        if not 1 <= self.tracerThreshold <= 100:
            raise ValueError("TRACER_THRESHOLD must be between 1 and 100")
        if not 1 <= self.sqsWaitSeconds <= 20:
            raise ValueError("SQS_WAIT_SECONDS must be between 1 and 20")
