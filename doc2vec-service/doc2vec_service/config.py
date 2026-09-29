import os
from dataclasses import dataclass

from dotenv import load_dotenv
from sqlalchemy.engine import make_url

load_dotenv()

# These values belong to this historical flow and do not change by environment.
OPENSEARCH_PORT = 443
OPENSEARCH_SERVICE = "es"
OPENSEARCH_BATCH_SIZE = 1_000
WORKER_BATCH_SIZE = 30
HISTORICAL_WORKER_COUNT = 100
RETRY_SECONDS = 5
TITAN_EMBEDDING_TYPE = 2


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
    pgDbUser: str
    pgDbPassword: str
    pgDbHost: str
    pgDbDatabase: str
    pgDbPort: int
    awsRegion: str
    iamAccessRole: str
    opensearchHost: str
    opensearchPort: int
    opensearchService: str
    sourceQueueUrl: str
    nextQueueUrl: str
    occurrenceAlias: str
    catalogAlias: str
    opensearchBatchSize: int
    workerBatchSize: int
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

        databaseUrl = os.getenv("DATABASE_URL", "").strip()
        parsedDatabaseUrl = make_url(databaseUrl) if databaseUrl else None
        config = cls(
            databaseUrl=databaseUrl,
            pgDbUser=(parsedDatabaseUrl.username or "") if parsedDatabaseUrl else "",
            pgDbPassword=(
                parsedDatabaseUrl.password or ""
            ) if parsedDatabaseUrl else "",
            pgDbHost=(parsedDatabaseUrl.host or "") if parsedDatabaseUrl else "",
            pgDbDatabase=(
                parsedDatabaseUrl.database or ""
            ) if parsedDatabaseUrl else "",
            pgDbPort=(parsedDatabaseUrl.port or 5432) if parsedDatabaseUrl else 5432,
            awsRegion=os.getenv("AWS_REGION", "us-east-1").strip(),
            iamAccessRole=os.getenv("IAM_ACCESS_ROLE", "").strip(),
            opensearchHost=os.getenv("OPENSEARCH_HOST", "").strip(),
            opensearchPort=OPENSEARCH_PORT,
            opensearchService=OPENSEARCH_SERVICE,
            sourceQueueUrl=os.getenv("DOC2VEC_QUEUE_URL", "").strip(),
            nextQueueUrl=os.getenv("SENTENCE_SIMILARITY_QUEUE_URL", "").strip(),
            occurrenceAlias=os.getenv(
                "OPENSEARCH_OCCURRENCE_ALIAS", "sentence_occurrences"
            ).strip(),
            catalogAlias=os.getenv(
                "OPENSEARCH_CATALOG_ALIAS", "sentence_semantic_catalog"
            ).strip(),
            opensearchBatchSize=OPENSEARCH_BATCH_SIZE,
            workerBatchSize=WORKER_BATCH_SIZE,
            historicalWorkerCount=HISTORICAL_WORKER_COUNT,
            retrySeconds=RETRY_SECONDS,
            vectorDimension=readInteger("VECTOR_DIMENSION", 512),
            titanEmbeddingType=TITAN_EMBEDDING_TYPE,
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
        if self.historicalWorkerCount > 100:
            raise ValueError(
                "HISTORICAL_WORKER_COUNT cannot be greater than 100"
            )
        if self.vectorDimension != 512:
            raise ValueError("VECTOR_DIMENSION must be 512 for Titan embeddings")
        if not 1 <= self.sqsWaitSeconds <= 20:
            raise ValueError("SQS_WAIT_SECONDS must be between 1 and 20")
