"""Fixed values shared by the Sentence Similarity OpenSearch flow."""

# Database
DATABASE_POOL_SIZE = 30
DATABASE_MAX_OVERFLOW = 10
TITAN_MATCH_WRITE_BATCH_SIZE = 3000  # Maximum target matches sent to the SQL function per call.

# OpenSearch
OPENSEARCH_PORT = 443
OPENSEARCH_SERVICE = "es"
SENTENCE_OCCURRENCE_ALIAS = "sentence_occurrences"
ASYLEE_CATALOG_ALIAS = "sentence_semantic_catalog_asylee"
REFUGEE_CATALOG_ALIAS = "sentence_semantic_catalog_refugee"
OPENSEARCH_TERMS_BATCH_SIZE = 1000  # Keys or hits handled per OpenSearch page.
VECTOR_DIMENSION = 512
VECTOR_FIELD = "sentenceContentVector"
MATCH_THRESHOLD = 76  # Minimum sentence-catalog similarity percentage.
MINIMUM_MATCH_WORD_COUNT = 5  # Shorter sentences do not create sentence matches.
IVF_NPROBES = 64  # IVF clusters searched by each sentence-catalog query.

# Processing
DATABASE_SENTENCE_LOAD_BATCH_SIZE = 1000  # Sentence rows loaded in each PostgreSQL query.
THREAD_WORKER_COUNT = 30  # Source sentences processed at the same time.
RETRY_SECONDS = 5  # Seconds to wait before retrying a failed operation.
OCCURRENCE_READINESS_WAIT_SECONDS = 30  # One-time wait for newly indexed occurrences.

# SQS
SQS_WAIT_SECONDS = 10
SQS_VISIBILITY_SECONDS = 7200  # Two-hour message visibility timeout.
