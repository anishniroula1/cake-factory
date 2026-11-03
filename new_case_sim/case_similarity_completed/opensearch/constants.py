"""Fixed values shared by the Case Similarity OpenSearch flow."""

# Database
DATABASE_POOL_SIZE = 20
DATABASE_MAX_OVERFLOW = 10

# Case rules
MIN_SIMILAR_CASE_COUNT = 2  # Sentence-pair count required for a similar case.
MIN_TAH_CASE_COUNT = 3  # Sentence-pair count required for a TAH case.

# Retry and SQS
RETRY_SECONDS = 5
MAX_ATTEMPTS = 3
SQS_WAIT_SECONDS = 10
SQS_VISIBILITY_SECONDS = 300

# Kafka
TAH_TOPIC = "rfs.ata.text_analytics.hit"
DEFAULT_APPLICATION_URL = "https://ui.rfs-ata.uscis.dhs.gov/ui/main/"
