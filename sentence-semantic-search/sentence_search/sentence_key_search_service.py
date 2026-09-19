import re

from sentence_search.opensearch_store import OpenSearchStore

SENTENCE_KEY_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class SentenceKeySearchService:
    def __init__(self, opensearch: OpenSearchStore):
        """Save the OpenSearch store used by sentence-key search."""

        self.opensearch = opensearch

    def search(
        self,
        application_id: str,
        sentence_key: str,
        analysis_group: str,
        threshold: int,
    ) -> dict:
        """Return all exact and similar sentences with their scores.

        Input: one SHA-256 sentence key and application A1.
        Output: every matching sentence outside A1 with exact or similar type.
        """

        sentence_key = self.validate_source_sentence(
            application_id,
            sentence_key,
            analysis_group,
        )
        sentence_key, search_keys, score_by_key = self.matching_keys(
            sentence_key, threshold
        )
        occurrences = self.opensearch.matching_occurrences(
            search_keys,
            application_id,
            analysis_group,
        )
        matches = self.add_match_details(
            occurrences,
            sentence_key,
            score_by_key,
        )
        exact_count = sum(match["matchType"] == "exact" for match in matches)
        similar_count = len(matches) - exact_count

        return {
            "applicationId": application_id,
            "analysisGroup": analysis_group,
            "sentenceKey": sentence_key,
            "thresholdPercentage": threshold,
            "matchingCountScope": "allResults",
            "directMatchingKeyCount": len(search_keys) - 1,
            "exactMatchCount": exact_count,
            "similarMatchCount": similar_count,
            "totalMatches": len(matches),
            "returnedMatches": len(matches),
            "matches": matches,
            "neuralSearchUsed": False,
        }

    def validate_source_sentence(
        self,
        application_id: str,
        sentence_key: str,
        analysis_group: str,
    ) -> str:
        """Require an eligible source sentence from the requested application.

        Input: application A1, a saved key, and analysis group Asylee.
        Output: the normalized key when its source sentence can be matched.
        """

        sentence_key = sentence_key.strip().lower()
        if not SENTENCE_KEY_PATTERN.fullmatch(sentence_key):
            raise ValueError("sentenceKey must be a 64-character SHA-256 value")
        if not self.opensearch.eligible_application_sentence_exists(
            application_id,
            sentence_key,
            analysis_group,
        ):
            raise ValueError(
                "sentenceKey is not an eligible sentence for this "
                "application and analysisGroup"
            )
        return sentence_key

    def matching_keys(
        self,
        sentence_key: str,
        threshold: int,
    ) -> tuple:
        """Return the source key, matching keys, and scores.

        Input: a saved sentence key and threshold 90.
        Output: exact and similar catalog keys with cosine percentages.
        """

        sentence_key = sentence_key.strip().lower()
        if not SENTENCE_KEY_PATTERN.fullmatch(sentence_key):
            raise ValueError("sentenceKey must be a 64-character SHA-256 value")
        if not 1 <= threshold <= 100:
            raise ValueError("threshold must be between 1 and 100")

        source_vector = self.opensearch.catalog_vector(sentence_key)
        catalog_matches = self.opensearch.catalog_matches(
            sentence_key,
            source_vector,
            threshold,
        )
        score_by_key = {sentence_key: 100.0}
        similar_keys = []
        for match in catalog_matches:
            matching_key = match["sentenceKey"]
            if matching_key == sentence_key:
                continue
            percentage = float(match["similarityPercentage"])
            if percentage < threshold:
                continue
            similar_keys.append(matching_key)
            score_by_key[matching_key] = percentage

        search_keys = [sentence_key, *sorted(set(similar_keys))]
        return sentence_key, search_keys, score_by_key

    @staticmethod
    def add_match_details(
        occurrences: list,
        sentence_key: str,
        score_by_key: dict,
    ) -> list:
        """Add exact or similar type and score to occurrence records."""

        matches = []
        for occurrence in occurrences:
            occurrence_key = occurrence["sentenceKey"]
            match_type = (
                "exact" if occurrence_key == sentence_key else "similar"
            )
            matches.append(
                {
                    **occurrence,
                    "matchPercentage": score_by_key[occurrence_key],
                    "matchType": match_type,
                }
            )
        return matches
