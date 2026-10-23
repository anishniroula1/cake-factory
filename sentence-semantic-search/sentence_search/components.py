from sentence_search.all_sentence_summary_service import AllSentenceSummaryService
from sentence_search.application_match_service import ApplicationMatchService
from sentence_search.config import config
from sentence_search.document_match_service import DocumentMatchService
from sentence_search.match_csv_export_service import MatchCsvExportService
from sentence_search.opensearch_store import OpenSearchStore
from sentence_search.paginated_sentence_key_search_service import (
    PaginatedSentenceKeySearchService,
)
from sentence_search.sentence_key_search_service import SentenceKeySearchService
from sentence_search.sentence_summary_service import SentenceSummaryService

opensearch_store = OpenSearchStore(config)
sentence_summary_service = SentenceSummaryService(opensearch_store)
all_sentence_summary_service = AllSentenceSummaryService(sentence_summary_service)
sentence_key_search_service = SentenceKeySearchService(opensearch_store)
paginated_sentence_key_search_service = PaginatedSentenceKeySearchService(
    opensearch_store,
    sentence_key_search_service,
)
application_match_service = ApplicationMatchService(
    opensearch_store,
    sentence_summary_service,
)
document_match_service = DocumentMatchService(
    opensearch_store,
    sentence_summary_service,
)
match_csv_export_service = MatchCsvExportService(
    opensearch_store,
    sentence_summary_service,
)
