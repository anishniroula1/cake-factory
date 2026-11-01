from concurrent.futures import ThreadPoolExecutor

from opensearchpy.exceptions import OpenSearchException

from doc_delete_job.opensearch import constants
from doc_delete_job.opensearch.contracts import OccurrenceRecord, TspId


class SentenceOccurrenceOpenSearchStore:
    """Read and delete occurrence documents, then find sentence keys still in use."""

    def __init__(self, client) -> None:
        self.client = client

    def load_document_occurrences(self, tsp_id: TspId) -> list[OccurrenceRecord]:
        """Load every occurrence belonging to one document.

        Input: tsp_id=8205
        Output: records containing each sentence key and analysis group.

        Search-after pagination is used because a document can contain more
        sentences than one OpenSearch response page.
        """

        occurrence_records: list[OccurrenceRecord] = []
        search_after = None
        while True:
            search_body = {
                "size": constants.OPENSEARCH_PAGE_SIZE,
                "track_total_hits": False,
                "_source": ["sentenceKey", "analysisGroup"],
                "query": {"term": {"tspId": tsp_id}},
                "sort": [{"globalId": {"order": "asc"}}],
            }
            if search_after is not None:
                search_body["search_after"] = search_after

            search_response = self.client.search(index=constants.SENTENCE_OCCURRENCE_ALIAS, body=search_body)
            occurrence_hits = search_response.get("hits", {}).get("hits", [])
            if not occurrence_hits:
                return occurrence_records

            for occurrence_hit in occurrence_hits:
                occurrence_source = occurrence_hit["_source"]
                occurrence_records.append(
                    {
                        "sentence_key": str(occurrence_source["sentenceKey"]),
                        "analysis_group": str(occurrence_source["analysisGroup"]),
                    }
                )

            if len(occurrence_hits) < constants.OPENSEARCH_PAGE_SIZE:
                return occurrence_records
            search_after = occurrence_hits[-1]["sort"]

    def delete_occurrences_by_tsp_id(self, tsp_id: TspId) -> int:
        """Delete every occurrence owned by one tsp_id in one operation.

        Input: tsp_id=8205
        Output: 2

        Occurrence ownership is document-based, so delete-by-query is used
        instead of creating one bulk delete action per globalId. Refresh makes
        the completed deletion visible to the following services immediately.
        """

        delete_response = self.client.delete_by_query(
            index=constants.SENTENCE_OCCURRENCE_ALIAS,
            body={"query": {"term": {"tspId": tsp_id}}},
            conflicts="proceed",
            refresh=True,
            wait_for_completion=True,
        )
        failures = delete_response.get("failures", [])
        version_conflict_count = int(delete_response.get("version_conflicts", 0))
        timed_out = bool(delete_response.get("timed_out", False))
        if failures or version_conflict_count or timed_out:
            raise OpenSearchException(
                "occurrence delete-by-query did not delete every matching document: "
                f"shard_failures={len(failures)}, version_conflicts={version_conflict_count}, timed_out={timed_out}"
            )
        return int(delete_response.get("deleted", 0))

    def find_sentence_keys_used_by_other_documents(
        self, sentence_keys: list[str], analysis_group: str, excluded_tsp_id: TspId
    ) -> set[str]:
        """Return keys used by another document in the same analysis group.

        Input: sentence_keys=["key-a", "key-b"], analysis_group="Asylee",
        excluded_tsp_id=8205
        Output: {"key-b"} when key-a belonged only to the deleted document but
        another Asylee occurrence still uses key-b.

        The current tsp_id is excluded because its occurrences are deliberately
        kept until catalog cleanup succeeds.
        """

        unique_sentence_keys = list(dict.fromkeys(sentence_keys))
        if not unique_sentence_keys:
            return set()

        worker_count = min(constants.CATALOG_REFERENCE_SEARCH_WORKERS, len(unique_sentence_keys))
        # Spread keys across at most 30 msearch requests. Each response still
        # maps one-to-one to its key, so shared and orphaned keys cannot mix.
        sentence_key_batches = [unique_sentence_keys[offset::worker_count] for offset in range(worker_count)]
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="catalog-reference") as executor:
            shared_key_batches = executor.map(
                lambda sentence_key_batch: self._find_shared_sentence_keys_batch(
                    sentence_key_batch, analysis_group, excluded_tsp_id
                ),
                sentence_key_batches,
            )
        return set().union(*shared_key_batches)

    def _find_shared_sentence_keys_batch(
        self, sentence_keys: list[str], analysis_group: str, excluded_tsp_id: TspId
    ) -> set[str]:
        """Use one msearch call to test whether each key has another owner."""

        msearch_body = []
        for sentence_key in sentence_keys:
            msearch_body.extend(
                [
                    {},
                    {
                        "size": 1,
                        "_source": False,
                        "track_total_hits": False,
                        "terminate_after": 1,
                        "query": {
                            "bool": {
                                "filter": [
                                    {"term": {"sentenceKey": sentence_key}},
                                    {"term": {"analysisGroup": analysis_group}},
                                ],
                                "must_not": [{"term": {"tspId": excluded_tsp_id}}],
                            }
                        },
                    },
                ]
            )

        msearch_response = self.client.msearch(index=constants.SENTENCE_OCCURRENCE_ALIAS, body=msearch_body)
        search_responses = msearch_response.get("responses", [])
        if len(search_responses) != len(sentence_keys):
            raise OpenSearchException(
                f"occurrence msearch returned {len(search_responses)} response(s) for {len(sentence_keys)} key(s)"
            )

        shared_sentence_keys = set()
        for sentence_key, search_response in zip(sentence_keys, search_responses, strict=True):
            if search_response.get("error"):
                raise OpenSearchException(f"occurrence msearch failed while checking sentenceKey={sentence_key}")
            if search_response.get("hits", {}).get("hits", []):
                shared_sentence_keys.add(sentence_key)
        return shared_sentence_keys
