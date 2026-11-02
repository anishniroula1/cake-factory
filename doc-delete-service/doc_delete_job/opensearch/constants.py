"""Fixed values used by the document-deletion OpenSearch flow."""

# Database
DATABASE_POOL_SIZE = 20
DATABASE_MAX_OVERFLOW = 10

# OpenSearch
OPENSEARCH_PORT = 443
OPENSEARCH_SERVICE = "es"
SENTENCE_OCCURRENCE_ALIAS = "sentence_occurrences"
ASYLEE_CATALOG_ALIAS = "sentence_semantic_catalog_asylee"
REFUGEE_CATALOG_ALIAS = "sentence_semantic_catalog_refugee"
OPENSEARCH_PAGE_SIZE = 1000  # Occurrences or sentence keys handled in one request.
CATALOG_REFERENCE_SEARCH_WORKERS = 30

# Document state written after enabled database deletion finishes.
DELETED_DOCUMENT_STATE = "DOC_DELETED"

# Retry
RETRY_SECONDS = 5
MAX_ATTEMPTS = 3

