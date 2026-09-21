from collections import defaultdict
from math import ceil

from sentence_search.opensearch_store import (
    CATALOG_PAGE_SIZE,
    VECTOR_FIELD,
    OpenSearchStore,
)
from sentence_search.search_utils import (
    cosine_percentage,
    decode_page_token,
    encode_page_token,
    minimum_opensearch_score,
)

MSEARCH_BATCH_SIZE = 100  # Source sentences sent in one OpenSearch msearch call.


class SentenceSummaryService:
    def __init__(self, opensearch: OpenSearchStore):
        """Save the OpenSearch store used by the application summary."""

        self.opensearch = opensearch

    def application_summary(
        self,
        application_id: str,
        analysis_group: str,
        threshold: int,
        page_size: int,
        next_token: str | None,
    ) -> dict:
        """Return one sentence page and counts for only that page.

        Input: application A1, analysis group Asylee, and page size 50.
        Output: up to 50 sentences with page-only match counts.
        """

        if not 1 <= threshold <= 100:
            raise ValueError("threshold must be between 1 and 100")

        if next_token:
            state = decode_page_token(next_token)
            self._validate_token(
                state,
                application_id,
                analysis_group,
                threshold,
                page_size,
            )
            after_global_id = state["afterGlobalId"]
            total_sentences = int(state["totalSentences"])
            returned_before = int(state["returnedSentences"])
            include_total = False
        else:
            after_global_id = None
            total_sentences = 0
            returned_before = 0
            include_total = True

        result = self.opensearch.application_occurrence_page(
            application_id,
            analysis_group,
            page_size,
            after_global_id,
            include_total,
        )
        if include_total:
            total_sentences = int(result["totalSentences"] or 0)

        sentences = result["sentences"]
        source_keys = sorted({sentence["sentenceKey"] for sentence in sentences})
        similar_keys_by_source = self._similar_keys_at_threshold(
            source_keys,
            threshold,
        )

        # Count all exact and similar candidates for this page in one
        # occurrence aggregation instead of one query per source sentence.
        candidate_keys = set(source_keys)
        for source_key in source_keys:
            candidate_keys.update(similar_keys_by_source[source_key])
        counts = self.opensearch.occurrence_counts_by_key(
            list(candidate_keys),
            application_id,
            analysis_group,
        )

        response_sentences = []
        section_counts = defaultdict(int)
        for sentence in sentences:
            source_key = sentence["sentenceKey"]
            similar_keys = similar_keys_by_source[source_key]
            exact_count = counts.get(source_key, 0)
            similar_count = sum(counts.get(key, 0) for key in similar_keys)
            total_count = exact_count + similar_count
            response_sentences.append(
                {
                    **sentence,
                    "exactMatchCount": exact_count,
                    "similarMatchCount": similar_count,
                    "totalMatchCount": total_count,
                    "directMatchingKeyCount": len(similar_keys),
                }
            )
            section_counts[sentence.get("sectionName", "")] += total_count

        returned_after = returned_before + len(response_sentences)
        new_token = None
        next_after = result["nextAfterGlobalId"]
        if next_after:
            new_token = encode_page_token(
                {
                    "type": "applicationSentenceSummary",
                    "applicationId": application_id,
                    "analysisGroup": analysis_group,
                    "threshold": threshold,
                    "pageSize": page_size,
                    "afterGlobalId": next_after,
                    "totalSentences": total_sentences,
                    "returnedSentences": returned_after,
                }
            )

        total_pages = ceil(total_sentences / page_size) if total_sentences else 0
        total_matching = sum(
            sentence["totalMatchCount"] for sentence in response_sentences
        )
        return {
            "applicationId": application_id,
            "analysisGroup": analysis_group,
            "thresholdPercentage": threshold,
            "summaryScope": "currentPage",
            "totalMatching": total_matching,
            "sectionMatches": self._section_matches(section_counts),
            "totalSentences": total_sentences,
            "returnedSentences": len(response_sentences),
            "matchingSentences": sum(
                sentence["totalMatchCount"] > 0 for sentence in response_sentences
            ),
            "pagination": {
                "page": (returned_before // page_size) + 1,
                "pageSize": page_size,
                "totalPages": total_pages,
                "hasPreviousPage": returned_before > 0,
                "hasNextPage": new_token is not None,
            },
            "nextToken": new_token,
            "sentences": response_sentences,
            "neuralSearchUsed": False,
        }

    def _similar_keys_at_threshold(
        self,
        source_keys: list,
        threshold: int,
    ) -> dict:
        """Find catalog matches for all source keys using batched msearch."""

        scores = self.matching_key_scores(source_keys, threshold)
        similar_keys = {}
        for source_key in source_keys:
            similar_keys[source_key] = sorted(
                key for key in scores[source_key] if key != source_key
            )
        return similar_keys

    def matching_key_scores(
        self,
        source_keys: list,
        threshold: int,
    ) -> dict:
        """Return matching catalog keys and scores for many source keys.

        Input: saved source sentence keys and threshold 90.
        Output: each source key mapped to exact and similar key percentages.
        """

        if not source_keys:
            return {}
        vectors = self.opensearch.catalog_vectors(source_keys)
        missing_keys = [key for key in source_keys if key not in vectors]
        if missing_keys:
            raise RuntimeError(
                f"Catalog vector does not exist for {missing_keys[0]}"
            )

        qualified = {key: {key: 100.0} for key in source_keys}
        pending = [(key, vectors[key], None) for key in source_keys]
        while pending:
            next_pending = []
            for offset in range(0, len(pending), MSEARCH_BATCH_SIZE):
                batch = pending[offset : offset + MSEARCH_BATCH_SIZE]
                bodies = []
                for source_key, vector, after_key in batch:
                    bodies.append(
                        self._catalog_search_body(
                            source_key,
                            vector,
                            threshold,
                            after_key,
                        )
                    )
                responses = self.opensearch.multi_search_catalog(bodies)
                if len(responses) != len(batch):
                    raise RuntimeError(
                        "OpenSearch returned an invalid msearch response"
                    )
                for state, response in zip(batch, responses):
                    source_key, vector, _ = state
                    result = self._catalog_result(response)
                    for bucket in result["buckets"]:
                        hit = bucket["sample"]["hits"]["hits"][0]
                        candidate_key = hit["_source"]["sentenceKey"]
                        if candidate_key == source_key:
                            continue
                        percentage = cosine_percentage(float(hit["_score"]))
                        if percentage >= threshold:
                            qualified[source_key][candidate_key] = percentage

                    next_after_key = result.get("after_key")
                    if (
                        next_after_key
                        and len(result["buckets"]) == CATALOG_PAGE_SIZE
                    ):
                        next_pending.append(
                            (source_key, vector, next_after_key)
                        )
            pending = next_pending

        return qualified

    def _catalog_search_body(
        self,
        source_key: str,
        vector: list,
        threshold: int,
        after_key: dict | None,
    ) -> dict:
        """Make one catalog query for an msearch request."""

        composite = {
            "size": CATALOG_PAGE_SIZE,
            "sources": [{"sentenceKey": {"terms": {"field": "sentenceKey"}}}],
        }
        if after_key:
            composite["after"] = after_key
        return {
            "size": 0,
            "track_total_hits": False,
            "query": {
                "bool": {
                    "should": [
                        {"term": {"sentenceKey": source_key}},
                        {
                            "knn": {
                                VECTOR_FIELD: {
                                    "vector": vector,
                                    "min_score": minimum_opensearch_score(
                                        threshold
                                    ),
                                    "method_parameters": {
                                        "nprobes": self.opensearch.config.ivf_nprobes,
                                    },
                                }
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            },
            "aggs": {
                "matches": {
                    "composite": composite,
                    "aggs": {
                        "sample": {
                            "top_hits": {
                                "size": 1,
                                "_source": ["sentenceKey", "sentenceContent"],
                            }
                        }
                    },
                }
            },
        }

    @staticmethod
    def _catalog_result(response: dict) -> dict:
        """Read one msearch result or show its OpenSearch error."""

        if "error" in response:
            raise RuntimeError(
                f"OpenSearch semantic search failed: {response['error']}"
            )
        return response["aggregations"]["matches"]

    @staticmethod
    def _section_matches(section_counts: dict) -> list:
        """Return section totals in a stable name order."""

        return [
            {
                "sectionName": section_name,
                "matchingCount": section_counts[section_name],
            }
            for section_name in sorted(section_counts)
        ]

    @staticmethod
    def _validate_token(
        state: dict,
        application_id: str,
        analysis_group: str,
        threshold: int,
        page_size: int,
    ):
        """Stop a token from being reused for a different summary request."""

        expected = {
            "type": "applicationSentenceSummary",
            "applicationId": application_id,
            "analysisGroup": analysis_group,
            "threshold": threshold,
            "pageSize": page_size,
        }
        for name, value in expected.items():
            if state.get(name) != value:
                raise ValueError("nextToken does not belong to this request")
        required = {"totalSentences", "returnedSentences"}
        if not required.issubset(state):
            raise ValueError("Invalid nextToken")
