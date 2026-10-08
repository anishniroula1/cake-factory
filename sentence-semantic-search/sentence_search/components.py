from sentence_search.all_sentence_summary_service import AllSentenceSummaryService
from sentence_search.application_match_service import ApplicationMatchService
from sentence_search.config import config
from sentence_search.deletion_service import SentenceDeletionService
from sentence_search.opensearch_store import OpenSearchStore
from sentence_search.paginated_sentence_key_search_service import (
    PaginatedSentenceKeySearchService,
)
from sentence_search.seed_service import SeedService
from sentence_search.sentence_key_search_service import SentenceKeySearchService
from sentence_search.sentence_service import SentenceService
from sentence_search.sentence_summary_service import SentenceSummaryService
from sentence_search.tracer_seed_service import TracerSeedService

opensearch_store = OpenSearchStore(config)
sentence_summary_service = SentenceSummaryService(opensearch_store)
all_sentence_summary_service = AllSentenceSummaryService(sentence_summary_service)
sentence_service = SentenceService(opensearch_store)
seed_service = SeedService(
    config,
    opensearch_store,
)
tracer_seed_service = TracerSeedService(config, opensearch_store)
deletion_service = SentenceDeletionService(opensearch_store)
sentence_key_search_service = SentenceKeySearchService(opensearch_store)
paginated_sentence_key_search_service = PaginatedSentenceKeySearchService(
    opensearch_store,
    sentence_key_search_service,
)
application_match_service = ApplicationMatchService(
    opensearch_store,
    sentence_summary_service,
)
