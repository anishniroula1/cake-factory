from sentence_search.opensearch_store import OpenSearchStore
from sentence_search.sentence_summary_service import SentenceSummaryService

SOURCE_PAGE_SIZE = 1_000  # Source application sentences read per internal page.


def application_match_sort_key(item: tuple) -> tuple:
    """Rank applications by pair count and then application ID."""

    application_id, counts = item
    return (-int(counts["matchCount"]), str(application_id))


def target_match_sort_key(match: dict) -> tuple:
    """Put the strongest targets first and keep equal scores stable."""

    return (
        -float(match["matchPercentage"]),
        int(match["target"]["globalId"]),
    )


def source_match_sort_key(match: dict) -> tuple:
    """Put source groups with the strongest target first."""

    strongest_percentage = float(match["targets"][0]["matchPercentage"])
    return (-strongest_percentage, int(match["source"]["globalId"]))


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
    ) -> dict:
        """Rank other applications by every matching sentence pair.

        Input: application A1, analysis group Asylee, and threshold 90.
        Output: all matching applications and their source-to-target pairs.
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
            source_sentences,
        )

    def compare_tsp(
        self,
        tsp_id: str,
        analysis_group: str,
        threshold: int,
    ) -> dict:
        """Rank applications matching the eligible sentences in one document.

        Input: TSP document T1, analysis group Asylee, and threshold 90.
        Output: all applications and pairs originating from document T1.
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
            source_sentences,
        )
        return {"tspId": tsp_id, **result}

    def _compare_source_sentences(
        self,
        application_id: str,
        analysis_group: str,
        threshold: int,
        source_sentences: list,
    ) -> dict:
        """Build the complete ranked result from eligible source sentences."""

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
        selected_application_ids = [item[0] for item in ranked_applications]
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
        matches_by_application = self._group_sentence_matches(
            target_sentences,
            relations_by_target,
            source_sentences_by_key,
        )

        applications = []
        for target_application_id, counts in ranked_applications:
            matches = matches_by_application.get(target_application_id, [])
            applications.append(
                {
                    "applicationId": target_application_id,
                    **counts,
                    "matches": matches,
                }
            )

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
        """Return target keys used by the matching applications."""

        selected = set(application_ids)
        return {
            row["sentenceKey"]
            for row in count_rows
            if row["applicationId"] in selected
        }

    @staticmethod
    def _group_sentence_matches(
        target_sentences: list,
        relations_by_target: dict,
        source_sentences_by_key: dict,
    ) -> dict:
        """Group every unique target match under its source sentence."""

        source_groups_by_application = {}
        seen_pairs_by_application = {}
        for target in target_sentences:
            # Composite aggregation keys are normalized to strings, while
            # occurrence documents can return applicationId as an integer.
            # Use one string key so the counts and detailed pairs join.
            target_application_id = str(target["applicationId"])
            source_groups = source_groups_by_application.setdefault(
                target_application_id,
                {},
            )
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
                    source_group = source_groups.setdefault(
                        int(source["globalId"]),
                        {
                            "source": source,
                            "targetCount": 0,
                            "exactTargetCount": 0,
                            "similarTargetCount": 0,
                            "targets": [],
                        },
                    )
                    source_group["targets"].append(
                        {
                            "target": target,
                            "matchType": relation["matchType"],
                            "matchPercentage": relation["matchPercentage"],
                        }
                    )
                    source_group["targetCount"] += 1
                    if relation["matchType"] == "exact":
                        source_group["exactTargetCount"] += 1
                    else:
                        source_group["similarTargetCount"] += 1

        matches_by_application = {}
        for application_id, source_groups in source_groups_by_application.items():
            matches = list(source_groups.values())
            for match in matches:
                match["targets"].sort(key=target_match_sort_key)
            matches.sort(key=source_match_sort_key)
            matches_by_application[application_id] = matches
        return matches_by_application
