from sentence_search.opensearch_store import OpenSearchStore
from sentence_search.search_utils import decode_page_token, encode_page_token
from sentence_search.sentence_key_search_service import SentenceKeySearchService


class PaginatedSentenceKeySearchService:
    def __init__(
        self,
        opensearch: OpenSearchStore,
        sentence_search: SentenceKeySearchService,
    ):
        """Save the store and shared sentence-key search service."""

        self.opensearch = opensearch
        self.sentence_search = sentence_search

    def search(
        self,
        application_id: str,
        sentence_key: str,
        analysis_group: str,
        threshold: int,
        page_size: int,
        next_token: str | None,
    ) -> dict:
        """Return one catalog page with all eligible occurrences per match.

        Input: one saved sentence key, threshold 90, and page size 50.
        Output: up to 50 unique matches with every occurrence for those keys.
        """

        sentence_key = self.sentence_search.validate_source_sentence(
            application_id,
            sentence_key,
            analysis_group,
        )
        if not 1 <= threshold <= 100:
            raise ValueError("threshold must be between 1 and 100")

        if next_token:
            state = decode_page_token(next_token)
            self._validate_token(
                state,
                application_id,
                sentence_key,
                analysis_group,
                threshold,
                page_size,
            )
            after_key = state["afterKey"]
            returned_before = int(state["returnedMatches"])
        else:
            after_key = None
            returned_before = 0

        source_vector = self.opensearch.catalog_vector(sentence_key)
        matches, next_after_key = self._catalog_page_with_occurrences(
            application_id,
            sentence_key,
            source_vector,
            analysis_group,
            threshold,
            page_size,
            after_key,
        )

        returned_after = returned_before + len(matches)
        new_token = None
        if next_after_key:
            new_token = encode_page_token(
                {
                    "type": "sentenceKeySemanticSearch",
                    "applicationId": application_id,
                    "sentenceKey": sentence_key,
                    "analysisGroup": analysis_group,
                    "threshold": threshold,
                    "pageSize": page_size,
                    "afterKey": next_after_key,
                    "returnedMatches": returned_after,
                }
            )

        exact_count = sum(match["matchType"] == "exact" for match in matches)
        similar_count = len(matches) - exact_count
        return {
            "applicationId": application_id,
            "analysisGroup": analysis_group,
            "sentenceKey": sentence_key,
            "thresholdPercentage": threshold,
            "matchingCountScope": "currentCatalogPage",
            "directMatchingKeyCount": similar_count,
            "exactMatchCount": exact_count,
            "similarMatchCount": similar_count,
            "totalMatches": len(matches),
            "returnedMatches": len(matches),
            "returnedOccurrences": sum(
                match["occurrenceCount"] for match in matches
            ),
            "pagination": {
                "page": (returned_before // page_size) + 1,
                "pageSize": page_size,
                "hasPreviousPage": returned_before > 0,
                "hasNextPage": new_token is not None,
            },
            "nextToken": new_token,
            "matches": matches,
            "neuralSearchUsed": False,
        }

    def _catalog_page_with_occurrences(
        self,
        application_id: str,
        sentence_key: str,
        source_vector: list,
        analysis_group: str,
        threshold: int,
        page_size: int,
        after_key: dict | None,
    ) -> tuple:
        """Fill one page with catalog keys having eligible occurrences."""

        matches = []
        next_after_key = after_key
        while len(matches) < page_size:
            remaining = page_size - len(matches)
            page = self.opensearch.catalog_match_page(
                sentence_key,
                source_vector,
                threshold,
                remaining,
                next_after_key,
            )
            candidates = page["matches"]
            next_after_key = page["nextAfterKey"]
            if candidates:
                matches.extend(
                    self._matches_with_occurrences(
                        candidates,
                        application_id,
                        analysis_group,
                    )
                )
            if next_after_key is None:
                break
        return matches, next_after_key

    def _matches_with_occurrences(
        self,
        candidates: list,
        application_id: str,
        analysis_group: str,
    ) -> list:
        """Attach every eligible occurrence to each unique catalog match."""

        candidate_keys = [candidate["sentenceKey"] for candidate in candidates]
        occurrences = self.opensearch.matching_occurrences(
            candidate_keys,
            application_id,
            analysis_group,
        )
        occurrences_by_key = {key: [] for key in candidate_keys}
        for occurrence in occurrences:
            occurrences_by_key[occurrence["sentenceKey"]].append(occurrence)

        matches = []
        for candidate in candidates:
            occurrence_list = occurrences_by_key[candidate["sentenceKey"]]
            # A catalog key is not a match for this request when it has no
            # eligible occurrence in the requested analysis group.
            if not occurrence_list:
                continue
            matches.append(
                {
                    "sentenceKey": candidate["sentenceKey"],
                    "sentenceContent": candidate["sentenceContent"],
                    "matchPercentage": candidate["similarityPercentage"],
                    "matchType": candidate["matchType"],
                    "occurrenceCount": len(occurrence_list),
                    "occurrences": occurrence_list,
                }
            )
        return matches

    @staticmethod
    def _validate_token(
        state: dict,
        application_id: str,
        sentence_key: str,
        analysis_group: str,
        threshold: int,
        page_size: int,
    ):
        """Stop a token from being reused for a different search request."""

        expected = {
            "type": "sentenceKeySemanticSearch",
            "applicationId": application_id,
            "sentenceKey": sentence_key,
            "analysisGroup": analysis_group,
            "threshold": threshold,
            "pageSize": page_size,
        }
        for name, value in expected.items():
            if state.get(name) != value:
                raise ValueError("nextToken does not belong to this request")
        required = {"afterKey", "returnedMatches"}
        if not required.issubset(state):
            raise ValueError("Invalid nextToken")
