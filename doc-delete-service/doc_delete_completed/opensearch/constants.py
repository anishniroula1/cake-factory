"""Fixed values used by the document-deletion OpenSearch flow."""

# Database
DATABASE_POOL_SIZE = 20
DATABASE_MAX_OVERFLOW = 10

# OpenSearch
SENTENCE_OCCURRENCE_ALIAS = "sentence_occurrences"
ASYLEE_CATALOG_ALIAS = "sentence_semantic_catalog_asylee"
REFUGEE_CATALOG_ALIAS = "sentence_semantic_catalog_refugee"
OPENSEARCH_PAGE_SIZE = 1000  # Occurrences or sentence keys handled in one request.

# Retry and SQS
RETRY_SECONDS = 5
MAX_ATTEMPTS = 3
SQS_WAIT_SECONDS = 10
SQS_VISIBILITY_SECONDS = 7200

