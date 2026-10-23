from math import ceil

from sentence_search.opensearch_store import OpenSearchStore
from sentence_search.search_utils import decode_page_token, encode_page_token
from sentence_search.sentence_summary_service import SentenceSummaryService

APPLICATION_PAGE_SIZE = 10  # Other applications returned on one public page.
SOURCE_PAGE_SIZE = 1_000  # Source application sentences read per internal page.


def application_match_sort_key(item: tuple) -> tuple:
    """Rank applications by pair count and then application ID."""

    application_id, counts = item
    return (-int(counts["matchCount"]), str(application_id))


def sentence_pair_sort_key(match: dict) -> tuple:
    """Put the strongest pairs first and keep equal scores stable."""

    return (
        -float(match["matchPercentage"]),
        int(match["source"]["globalId"]),
        int(match["target"]["globalId"]),
    )


class ApplicationMatchService:
    def __init__(
        self,
        opensearch: OpenSearchStore,
        sentence_summary: SentenceSummaryService,
    ):
        """Save the services used to compare one application with others."""

        self.opensearch = opensearch
        self.sentence_summary = sentence_summary

    def compare(
        self,
        application_id: str,
        analysis_group: str,
        threshold: int,
        next_token: str | None,
    ) -> dict:
        """Rank other applications by every matching sentence pair.

        Input: application A1, analysis group Asylee, and threshold 90.
        Output: ten applications and all source-to-target pairs for those apps.
        """

        if not 1 <= threshold <= 100:
            raise ValueError("threshold must be between 1 and 100")

        source_sentences = self._source_sentences(
            application_id,
            analysis_group,
        )
        return self._compare_source_sentences(
            application_id,
            analysis_group,
            threshold,
            next_token,
            source_sentences,
            "applicationSentenceMatches",
        )

    def compare_tsp(
        self,
        tsp_id: str,
        analysis_group: str,
        threshold: int,
        next_token: str | None,
    ) -> dict:
        """Rank applications matching the eligible sentences in one document.

        Input: TSP document T1, analysis group Asylee, and threshold 90.
        Output: ten applications and all pairs originating from document T1.
        """

        if not 1 <= threshold <= 100:
            raise ValueError("threshold must be between 1 and 100")

        source_sentences = self._tsp_source_sentences(tsp_id, analysis_group)
        if not source_sentences:
            raise ValueError(
                "tspId has no eligible sentences for this analysisGroup"
            )
        source_application_ids = {
            sentence["applicationId"] for sentence in source_sentences
        }
        if len(source_application_ids) != 1:
            raise RuntimeError("tspId belongs to more than one application")
        application_id = source_application_ids.pop()
        result = self._compare_source_sentences(
            application_id,
            analysis_group,
            threshold,
            next_token,
            source_sentences,
            "tspSentenceMatches",
            tsp_id,
        )
        return {"tspId": tsp_id, **result}

    def _compare_source_sentences(
        self,
        application_id: str,
        analysis_group: str,
        threshold: int,
        next_token: str | None,
        source_sentences: list,
        token_type: str,
        tsp_id: str | None = None,
    ) -> dict:
        """Build one ranked application page from eligible source sentences."""

        offset = 0
        if next_token:
            state = decode_page_token(next_token)
            self._validate_token(
                state,
                application_id,
                analysis_group,
                threshold,
                token_type,
                tsp_id,
            )
            offset = int(state["applicationOffset"])

        source_sentences_by_key = self._sentences_by_key(source_sentences)
        source_keys = sorted(source_sentences_by_key)
        scores_by_source = self.sentence_summary.matching_key_scores(
            source_keys,
            analysis_group,
            threshold,
        )
        relations_by_target = self._relations_by_target(scores_by_source)

        count_rows = self.opensearch.occurrence_counts_by_application_and_key(
            list(relations_by_target),
            application_id,
            analysis_group,
        )
        counts_by_application = self._application_counts(
            count_rows,
            relations_by_target,
            source_sentences_by_key,
        )
        ranked_applications = sorted(
            counts_by_application.items(),
            key=application_match_sort_key,
        )

        total_applications = len(ranked_applications)
        page_items = ranked_applications[offset : offset + APPLICATION_PAGE_SIZE]
        selected_application_ids = [item[0] for item in page_items]
        selected_key_set = self._selected_target_keys(
            count_rows,
            selected_application_ids,
        )
        target_sentences = (
            self.opensearch.matching_occurrences_for_applications(
                list(selected_key_set),
                selected_application_ids,
                application_id,
                analysis_group,
            )
        )
        matches_by_application = self._sentence_pairs(
            target_sentences,
            relations_by_target,
            source_sentences_by_key,
        )

        applications = []
        for target_application_id, counts in page_items:
            matches = matches_by_application.get(target_application_id, [])
            matches.sort(key=sentence_pair_sort_key)
            applications.append(
                {
                    "applicationId": target_application_id,
                    **counts,
                    "matches": matches,
                }
            )

        returned_after = offset + len(applications)
        new_token = None
        if returned_after < total_applications:
            token_state = {
                "type": token_type,
                "applicationId": application_id,
                "analysisGroup": analysis_group,
                "threshold": threshold,
                "pageSize": APPLICATION_PAGE_SIZE,
                "applicationOffset": returned_after,
            }
            if tsp_id is not None:
                token_state["tspId"] = tsp_id
            new_token = encode_page_token(
                token_state
            )

        total_pages = ceil(total_applications / APPLICATION_PAGE_SIZE)
        return {
            "applicationId": application_id,
            "analysisGroup": analysis_group,
            "thresholdPercentage": threshold,
            "countScope": "allSourceTargetSentencePairs",
            "totalMatchingApplications": total_applications,
            "totalMatchCount": sum(
                counts["matchCount"] for counts in counts_by_application.values()
            ),
            "returnedApplications": len(applications),
            "pagination": {
                "page": (offset // APPLICATION_PAGE_SIZE) + 1,
                "pageSize": APPLICATION_PAGE_SIZE,
                "totalPages": total_pages,
                "hasPreviousPage": offset > 0,
                "hasNextPage": new_token is not None,
            },
            "nextToken": new_token,
            "applications": applications,
            "neuralSearchUsed": False,
        }

    def _source_sentences(
        self,
        application_id: str,
        analysis_group: str,
    ) -> list:
        """Load every eligible source sentence using internal pages."""

        sentences = []
        after_global_id = None
        while True:
            page = self.opensearch.application_occurrence_page(
                application_id,
                analysis_group,
                SOURCE_PAGE_SIZE,
                after_global_id,
                False,
            )
            sentences.extend(page["sentences"])
            after_global_id = page["nextAfterGlobalId"]
            if after_global_id is None:
                return sentences

    def _tsp_source_sentences(
        self,
        tsp_id: str,
        analysis_group: str,
    ) -> list:
        """Load every eligible source sentence for one TSP document."""

        sentences = []
        after_global_id = None
        while True:
            page = self.opensearch.tsp_occurrence_page(
                tsp_id,
                analysis_group,
                SOURCE_PAGE_SIZE,
                after_global_id,
            )
            sentences.extend(page["sentences"])
            after_global_id = page["nextAfterGlobalId"]
            if after_global_id is None:
                return sentences

    @staticmethod
    def _sentences_by_key(sentences: list) -> dict:
        """Group source occurrences that share one catalog key."""

        grouped = {}
        for sentence in sentences:
            grouped.setdefault(sentence["sentenceKey"], []).append(sentence)
        return grouped

    @staticmethod
    def _relations_by_target(scores_by_source: dict) -> dict:
        """Map each target key to every matching source key and score."""

        relations = {}
        for source_key in sorted(scores_by_source):
            for target_key, percentage in scores_by_source[source_key].items():
                relations.setdefault(target_key, []).append(
                    {
                        "sourceKey": source_key,
                        "matchPercentage": float(percentage),
                        "matchType": (
                            "exact" if source_key == target_key else "similar"
                        ),
                    }
                )
        return relations

    @staticmethod
    def _application_counts(
        count_rows: list,
        relations_by_target: dict,
        source_sentences_by_key: dict,
    ) -> dict:
        """Calculate pair counts from source and target occurrence counts."""

        counts_by_application = {}
        for row in count_rows:
            application_id = row["applicationId"]
            target_count = int(row["occurrenceCount"])
            counts = counts_by_application.setdefault(
                application_id,
                {
                    "matchCount": 0,
                    "exactMatchCount": 0,
                    "similarMatchCount": 0,
                },
            )
            for relation in relations_by_target.get(row["sentenceKey"], []):
                source_count = len(
                    source_sentences_by_key[relation["sourceKey"]]
                )
                pair_count = source_count * target_count
                counts["matchCount"] += pair_count
                if relation["matchType"] == "exact":
                    counts["exactMatchCount"] += pair_count
                else:
                    counts["similarMatchCount"] += pair_count
        return counts_by_application

    @staticmethod
    def _selected_target_keys(
        count_rows: list,
        application_ids: list,
    ) -> set:
        """Return target keys used by the selected application page."""

        selected = set(application_ids)
        return {
            row["sentenceKey"]
            for row in count_rows
            if row["applicationId"] in selected
        }

    @staticmethod
    def _sentence_pairs(
        target_sentences: list,
        relations_by_target: dict,
        source_sentences_by_key: dict,
    ) -> dict:
        """Build every unique source-global-ID to target-global-ID pair."""

        matches_by_application = {}
        seen_pairs_by_application = {}
        for target in target_sentences:
            target_application_id = target["applicationId"]
            matches = matches_by_application.setdefault(target_application_id, [])
            seen_pairs = seen_pairs_by_application.setdefault(
                target_application_id,
                set(),
            )
            for relation in relations_by_target.get(target["sentenceKey"], []):
                for source in source_sentences_by_key[relation["sourceKey"]]:
                    pair = (int(source["globalId"]), int(target["globalId"]))
                    if pair in seen_pairs:
                        continue
                    seen_pairs.add(pair)
                    matches.append(
                        {
                            "source": source,
                            "target": target,
                            "matchType": relation["matchType"],
                            "matchPercentage": relation["matchPercentage"],
                        }
                    )
        return matches_by_application

    @staticmethod
    def _validate_token(
        state: dict,
        application_id: str,
        analysis_group: str,
        threshold: int,
        token_type: str = "applicationSentenceMatches",
        tsp_id: str | None = None,
    ):
        """Stop a token from being reused for another comparison."""

        expected = {
            "type": token_type,
            "applicationId": application_id,
            "analysisGroup": analysis_group,
            "threshold": threshold,
            "pageSize": APPLICATION_PAGE_SIZE,
        }
        if tsp_id is not None:
            expected["tspId"] = tsp_id
        for name, value in expected.items():
            if state.get(name) != value:
                raise ValueError("nextToken does not belong to this request")
        offset = state.get("applicationOffset")
        if (
            type(offset) is not int
            or offset < 0
            or offset % APPLICATION_PAGE_SIZE != 0
        ):
            raise ValueError("Invalid nextToken")
