"""Fixed values shared by the Case Similarity OpenSearch flow."""

# Database
DATABASE_POOL_SIZE = 20
DATABASE_MAX_OVERFLOW = 10

# Case rules
MIN_SIMILAR_CASE_COUNT = 2  # Sentence-pair count required for a similar case.
MIN_TAH_CASE_COUNT = 3  # Sentence-pair count required for a TAH case.

# Retry and SQS
RETRY_SECONDS = 5  # Seconds to wait before trying a failed operation again.
MAX_ATTEMPTS = 3  # Total attempts allowed for one retryable operation.
SQS_WAIT_SECONDS = 10  # Seconds one SQS receive request waits for a message.
SQS_VISIBILITY_SECONDS = 7200  # Keep a received message hidden for two hours while it is processed.
SQS_MESSAGE_BATCH_SIZE = 100  # Maximum source messages grouped into one Case Similarity batch.
SQS_RECEIVE_BATCH_SIZE = 10  # Maximum messages requested in one SQS receive call.
SQS_BATCH_WAIT_SECONDS = 15  # Wait this long for more messages before processing a partial batch.
SQS_SEND_BATCH_SIZE = 10  # Maximum messages sent to the next queue in one SQS batch call.

# Kafka
TAH_TOPIC = "rfs.ata.text_analytics.hit"
DEFAULT_APPLICATION_URL = "https://ui.rfs-ata.uscis.dhs.gov/ui/main/"
