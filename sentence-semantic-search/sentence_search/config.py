import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _integer(name: str, default: int, minimum: int = 1) -> int:
    """Read a positive whole number from the environment."""

    raw_value = os.getenv(name, str(default)).strip()
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be a whole number") from error
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


@dataclass(frozen=True)
class Config:
    aws_region: str
    iam_access_role: str
    opensearch_host: str
    opensearch_port: int
    opensearch_service: str
    semantic_model_id: str
    ivf_model_id: str
    ivf_nprobes: int
    ingest_pipeline: str
    occurrence_index: str
    occurrence_alias: str
    catalog_index: str
    catalog_alias: str
    index_shards: int
    index_replicas: int
    vector_dimension: int
    seed_batch_size: int
    seed_workers: int
    match_threshold: int

    @classmethod
    def from_environment(cls):
        """Build the application settings from environment variables."""

        config = cls(
            aws_region=os.getenv("AWS_REGION", "us-east-1").strip(),
            iam_access_role=os.getenv("IAM_ACCESS_ROLE", "").strip(),
            opensearch_host=os.getenv("OPENSEARCH_HOST", "").strip(),
            opensearch_port=_integer("OPENSEARCH_PORT", 443),
            opensearch_service=os.getenv(
                "OPENSEARCH_SERVICE",
                "es",
            ).strip(),
            semantic_model_id=os.getenv(
                "OPENSEARCH_SEMANTIC_MODEL_ID",
                "",
            ).strip(),
            ivf_model_id=os.getenv(
                "OPENSEARCH_IVF_MODEL_ID",
                "",
            ).strip(),
            ivf_nprobes=_integer("OPENSEARCH_IVF_NPROBES", 64),
            ingest_pipeline=os.getenv(
                "OPENSEARCH_INGEST_PIPELINE",
                "sentence_bedrock_embedding_pipeline",
            ).strip(),
            occurrence_index=os.getenv(
                "OPENSEARCH_OCCURRENCE_INDEX",
                "sentence_occurrences-v1",
            ).strip(),
            occurrence_alias=os.getenv(
                "OPENSEARCH_OCCURRENCE_ALIAS",
                "sentence_occurrences",
            ).strip(),
            catalog_index=os.getenv(
                "OPENSEARCH_CATALOG_INDEX",
                "sentence_semantic_catalog-ivf-pq-v1",
            ).strip(),
            catalog_alias=os.getenv(
                "OPENSEARCH_CATALOG_ALIAS",
                "sentence_semantic_catalog_ivf_pq",
            ).strip(),
            index_shards=_integer("INDEX_SHARDS", 3),
            index_replicas=_integer("INDEX_REPLICAS", 1, 0),
            vector_dimension=_integer("VECTOR_DIMENSION", 512),
            seed_batch_size=_integer("SEED_BATCH_SIZE", 100),
            seed_workers=_integer("SEED_WORKERS", 4),
            match_threshold=_integer("MATCH_THRESHOLD", 90),
        )
        config.validate()
        return config

    def validate(self):
        """Stop early when settings would create an invalid application."""

        if not self.opensearch_host:
            raise ValueError("OPENSEARCH_HOST is required")
        if not self.semantic_model_id:
            raise ValueError("OPENSEARCH_SEMANTIC_MODEL_ID is required")
        if not self.ivf_model_id:
            raise ValueError("OPENSEARCH_IVF_MODEL_ID is required")
        if not self.ingest_pipeline:
            raise ValueError("OPENSEARCH_INGEST_PIPELINE is required")
        if self.occurrence_index == self.catalog_index:
            raise ValueError("The occurrence and catalog indexes must differ")
        if self.occurrence_alias == self.catalog_alias:
            raise ValueError("The occurrence and catalog aliases must differ")
        if not 1 <= self.match_threshold <= 100:
            raise ValueError("MATCH_THRESHOLD must be between 1 and 100")
        if self.vector_dimension != 512:
            raise ValueError("VECTOR_DIMENSION must be 512 for this Titan model setup")
        if self.seed_workers > 16:
            raise ValueError("SEED_WORKERS must be between 1 and 16")


config = Config.from_environment()
