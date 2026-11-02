"""Fixed values shared by the Doc2Vec OpenSearch flow."""

# Database
DATABASE_POOL_SIZE = 30
DATABASE_MAX_OVERFLOW = 10
TITAN_EMBEDDING_TYPE = 2

# OpenSearch
OPENSEARCH_PORT = 443
OPENSEARCH_SERVICE = "es"
SENTENCE_OCCURRENCE_ALIAS = "sentence_occurrences"
ASYLEE_CATALOG_ALIAS = "sentence_semantic_catalog_asylee"
REFUGEE_CATALOG_ALIAS = "sentence_semantic_catalog_refugee"
TRAINING_ALIAS = "sentence_semantic_training"
TRACER_ALIAS = "tracer_form_language_catalog"
OPENSEARCH_BATCH_SIZE = 1000
OPENSEARCH_BULK_RETRY_COUNT = 30  # Retries after the first bulk request fails.
VECTOR_DIMENSION = 512

# Processing
LIVE_DATABASE_BATCH_SIZE = 1000  # Sentence records flushed per database batch.
DOCUMENT_SENTENCE_PAGE_SIZE = 500  # Saved sentence records loaded per query.
WORKER_BATCH_SIZE = 30  # Sentences handled by one historical worker job.
THREAD_WORKER_COUNT = 100  # Historical worker jobs allowed at the same time.
TRAIN_MODEL_WORKER_COUNT = 16  # Training-index jobs allowed at the same time.
RETRY_SECONDS = 5  # Seconds to wait before retrying a failed operation.
MINIMUM_MATCH_WORD_COUNT = 5  # Shorter sentences do not train the IVF model.

# SQS
SQS_WAIT_SECONDS = 10
SQS_VISIBILITY_SECONDS = 7200
SQS_SEND_BATCH_SIZE = 10  # Amazon SQS accepts at most ten messages in one batch request.

# Refugee referral documents whose RRF sections are filtered before splitting.
CASE_REFUGEE_SECTION_NAMES = [
    "Embassy Referral Document",
    "New Referral Document",
    "NGO Referral",
    "Referral Document",
    "US Embassy Referral",
    "UNHCR Referral",
    "US Embassy Referral Document",
    "P2 Referral",
    "RRF",
    "P1-RRF",
    "P2-RRF",
]

