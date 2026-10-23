from sentence_search.opensearch_store import OpenSearchStore
from sentence_search.sentence_summary_service import SentenceSummaryService

SOURCE_PAGE_SIZE = 1000  # Source document sentences read per internal page.


def document_match_sort_key(item: tuple) -> tuple:
    """Rank documents by pair count and then TSP ID."""

    tsp_id, counts = item
    return (-int(counts["matchCount"]), str(tsp_id))


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


class DocumentMatchService:
    def __init__(
        self,
        opensearch: OpenSearchStore,
        sentence_summary: SentenceSummaryService,
    ):
        """Save the services used to compare one document with others."""

        self.opensearch = opensearch
        self.sentence_summary = sentence_summary

    def compare(
        self,
        tsp_id: str,
        analysis_group: str,
        threshold: int,
    ) -> dict:
        """Rank target documents matching sentences from one source document.

        Input: tsp_id=101, analysis_group="Asylee", threshold=90.
        Output: every matching TSP document and its source-to-target pairs.
        """

        if not 1 <= threshold <= 100:
            raise ValueError("threshold must be between 1 and 100")

        source_sentences = self._source_sentences(tsp_id, analysis_group)
        if not source_sentences:
            raise ValueError(
                "tspId has no eligible sentences for this analysisGroup"
            )

        source_application_ids = {
            str(sentence["applicationId"]) for sentence in source_sentences
        }
        if len(source_application_ids) != 1:
            raise RuntimeError("tspId belongs to more than one application")
        source_application_id = source_application_ids.pop()

        source_sentences_by_key = self._sentences_by_key(source_sentences)
        source_keys = sorted(source_sentences_by_key)
        scores_by_source = self.sentence_summary.matching_key_scores(
            source_keys,
            analysis_group,
            threshold,
        )
        relations_by_target = self._relations_by_target(scores_by_source)

        count_rows = self.opensearch.occurrence_counts_by_tsp_and_key(
            list(relations_by_target),
            tsp_id,
            analysis_group,
        )
        counts_by_document = self._document_counts(
            count_rows,
            relations_by_target,
            source_sentences_by_key,
        )
        ranked_documents = sorted(
            counts_by_document.items(),
            key=document_match_sort_key,
        )

        selected_tsp_ids = [item[0] for item in ranked_documents]
        selected_keys = self._selected_target_keys(count_rows, selected_tsp_ids)
        target_sentences = self.opensearch.matching_occurrences_for_documents(
            list(selected_keys),
            selected_tsp_ids,
            tsp_id,
            analysis_group,
        )
        matches_by_document = self._group_sentence_matches(
            target_sentences,
            relations_by_target,
            source_sentences_by_key,
        )

        documents = []
        for target_tsp_id, counts in ranked_documents:
            documents.append(
                {
                    "tspId": target_tsp_id,
                    **counts,
                    "matches": matches_by_document.get(target_tsp_id, []),
                }
            )

        return {
            "tspId": tsp_id,
            "applicationId": source_application_id,
            "analysisGroup": analysis_group,
            "thresholdPercentage": threshold,
            "countScope": "allSourceTargetSentencePairs",
            "totalMatchingDocuments": len(documents),
            "totalMatchCount": sum(
                counts["matchCount"] for counts in counts_by_document.values()
            ),
            "returnedDocuments": len(documents),
            "documents": documents,
            "neuralSearchUsed": False,
        }

    def _source_sentences(self, tsp_id: str, analysis_group: str) -> list:
        """Load every eligible source sentence from the requested document."""

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
    def _document_counts(
        count_rows: list,
        relations_by_target: dict,
        source_sentences_by_key: dict,
    ) -> dict:
        """Calculate sentence-pair counts for each target TSP document."""

        counts_by_document = {}
        for row in count_rows:
            target_tsp_id = row["tspId"]
            target_count = int(row["occurrenceCount"])
            counts = counts_by_document.setdefault(
                target_tsp_id,
                {
                    "applicationId": row["applicationId"],
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
        return counts_by_document

    @staticmethod
    def _selected_target_keys(count_rows: list, tsp_ids: list) -> set:
        """Return target keys used by the selected target documents."""

        selected = set(tsp_ids)
        return {
            row["sentenceKey"]
            for row in count_rows
            if row["tspId"] in selected
        }

    @staticmethod
    def _group_sentence_matches(
        target_sentences: list,
        relations_by_target: dict,
        source_sentences_by_key: dict,
    ) -> dict:
        """Group target matches first by TSP document and then source sentence."""

        source_groups_by_document = {}
        seen_pairs_by_document = {}
        for target in target_sentences:
            target_tsp_id = str(target["tspId"])
            source_groups = source_groups_by_document.setdefault(
                target_tsp_id,
                {},
            )
            seen_pairs = seen_pairs_by_document.setdefault(target_tsp_id, set())
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

        matches_by_document = {}
        for tsp_id, source_groups in source_groups_by_document.items():
            matches = list(source_groups.values())
            for match in matches:
                match["targets"].sort(key=target_match_sort_key)
            matches.sort(key=source_match_sort_key)
            matches_by_document[tsp_id] = matches
        return matches_by_document
