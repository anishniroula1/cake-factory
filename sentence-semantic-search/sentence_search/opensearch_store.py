import base64
import binascii
import struct
import time

from opensearchpy import NotFoundError

from sentence_search.config import Config
from sentence_search.open_search_client import OpenSearchClient
from sentence_search.search_utils import (
    cosine_percentage,
    minimum_opensearch_score,
)

VECTOR_FIELD = "sentenceContentVector"
VECTOR_DIMENSION = 512
CATALOG_PAGE_SIZE = 1_000  # Catalog matches read from one aggregation page.
TERMS_BATCH_SIZE = 10_000  # Sentence keys sent in one terms filter.
VECTOR_BATCH_SIZE = 1_000  # Catalog vectors read in one OpenSearch request.
MATCH_RESULT_BATCH_SIZE = 5_000  # Occurrences read per internal search page.
APPLICATION_BUCKET_PAGE_SIZE = 1_000  # Application/key count buckets per page.
OCCURRENCE_SCAN_PAGE_SIZE = 1_000  # Occurrences checked per catalog join.
OCCURRENCE_SOURCE_FIELDS = [
    "applicationId",
    "tspId",
    "sectionName",
    "globalId",
    "sentIdLocal",
    "sentenceKey",
    "sourceType",
    "analysisGroup",
    "isCurrent",
]
CATALOG_SOURCE_FIELDS = [
    "sentenceKey",
    "sentenceContent",
    "wordCount",
    "isNonMatcher",
    "isTracer",
    "isFormLanguage",
]


def catalog_eligibility_filters() -> list:
    """Return eligibility rules that exist only in sentence catalogs.

    A matchable sentence is not a non-matcher, tracer, or form-language
    sentence and contains at least five meaningful words. DOC2Vec does not
    write classification fields on a new catalog row. A missing flag therefore
    has the same initial meaning as false; only an explicit true is excluded.
    """

    return [
        {
            "bool": {
                "must_not": [
                    {"term": {"isNonMatcher": True}},
                    {"term": {"isTracer": True}},
                    {"term": {"isFormLanguage": True}},
                ]
            }
        },
        {"range": {"wordCount": {"gt": 4}}},
    ]


def catalog_vector_query(config: Config, vector: list, threshold: int) -> dict:
    """Build a radial vector query with optional IVF search parameters."""

    query = {
        "vector": vector,
        "min_score": minimum_opensearch_score(threshold),
    }
    if config.ivf_nprobes_enabled:
        query["method_parameters"] = {"nprobes": config.ivf_nprobes}
    return query


class OpenSearchStore:
    def __init__(self, config: Config, client=None):
        """Create the store with an injected client or an AWS client."""

        self.config = config
        self.client = client or OpenSearchClient(config).create_client()

    def wait_until_ready(self, attempts: int = 60):
        """Wait for OpenSearch during application startup."""

        for _ in range(attempts):
            if self.client.ping():
                return
            time.sleep(2)
        raise RuntimeError("OpenSearch did not become ready")

    def catalog_vector(self, key: str, analysis_group: str) -> list:
        """Read one saved vector using fast binary doc values."""

        vectors = self.catalog_vectors([key], analysis_group)
        if key not in vectors:
            raise RuntimeError(f"Catalog vector does not exist for {key}")
        return vectors[key]

    def catalog_vectors(self, sentence_keys: list, analysis_group: str) -> dict:
        """Read saved vectors for sentence keys without loading `_source`.

        Input: two sentence keys already present in the catalog.
        Output: each sentence key mapped to its 512-number vector.
        """

        vectors = {}
        unique_keys = sorted(set(sentence_keys))
        for offset in range(0, len(unique_keys), VECTOR_BATCH_SIZE):
            batch = unique_keys[offset : offset + VECTOR_BATCH_SIZE]
            response = self.client.search(
                index=self.config.catalog_alias_for(analysis_group),
                body={
                    "size": len(batch),
                    "track_total_hits": False,
                    "_source": False,
                    "stored_fields": "_none_",
                    "docvalue_fields": [
                        "sentenceKey",
                        {
                            "field": VECTOR_FIELD,
                            "format": "binary",
                        },
                    ],
                    "query": {
                        "bool": {
                            "filter": [
                                {"ids": {"values": batch}},
                                *catalog_eligibility_filters(),
                            ]
                        }
                    },
                },
            )
            for hit in response["hits"]["hits"]:
                fields = hit.get("fields", {})
                key_values = fields.get("sentenceKey")
                vector_values = fields.get(VECTOR_FIELD)
                if key_values and vector_values:
                    vectors[str(key_values[0])] = self._decode_binary_vector(
                        vector_values[0]
                    )
        return vectors

    def catalog_records(
        self,
        sentence_keys: list,
        analysis_group: str,
    ) -> dict:
        """Load eligible catalog metadata keyed by sentence key.

        Occurrence documents do not contain sentence text, word count, or
        classification flags. This method reads those values from the catalog
        selected by the analysis group and leaves ineligible keys out.
        """

        records = {}
        unique_keys = sorted(set(sentence_keys))
        for offset in range(0, len(unique_keys), VECTOR_BATCH_SIZE):
            batch = unique_keys[offset : offset + VECTOR_BATCH_SIZE]
            response = self.client.search(
                index=self.config.catalog_alias_for(analysis_group),
                body={
                    "size": len(batch),
                    "track_total_hits": False,
                    "_source": CATALOG_SOURCE_FIELDS,
                    "query": {
                        "bool": {
                            "filter": [
                                {"ids": {"values": batch}},
                                *catalog_eligibility_filters(),
                            ]
                        }
                    },
                },
            )
            for hit in response["hits"]["hits"]:
                source = dict(hit["_source"])
                source.setdefault("isNonMatcher", False)
                source.setdefault("isTracer", False)
                source.setdefault("isFormLanguage", False)
                records[str(source["sentenceKey"])] = source
        return records

    def _decode_binary_vector(self, encoded_vector: str) -> list:
        """Convert an OpenSearch binary vector into float values."""

        try:
            vector_bytes = base64.b64decode(
                encoded_vector,
                validate=True,
            )
        except (binascii.Error, ValueError) as error:
            raise RuntimeError("OpenSearch returned an invalid vector") from error

        expected_bytes = VECTOR_DIMENSION * 4
        if len(vector_bytes) != expected_bytes:
            raise RuntimeError(
                "OpenSearch returned a vector with "
                f"{len(vector_bytes)} bytes; expected {expected_bytes}"
            )
        return list(
            struct.unpack(
                f"<{VECTOR_DIMENSION}f",
                vector_bytes,
            )
        )

    def catalog_matches(
        self,
        source_key: str,
        vector: list,
        threshold: int,
        analysis_group: str,
    ) -> list:
        """Return every unique catalog key meeting the cosine threshold."""

        matches = []
        after_key = None
        while True:
            page = self.catalog_match_page(
                source_key,
                vector,
                threshold,
                CATALOG_PAGE_SIZE,
                after_key,
                analysis_group,
            )
            matches.extend(page["matches"])
            after_key = page["nextAfterKey"]
            if after_key is None:
                break
        return matches

    def catalog_match_page(
        self,
        source_key: str,
        vector: list,
        threshold: int,
        page_size: int,
        after_key: dict | None,
        analysis_group: str,
    ) -> dict:
        """Return one unique catalog-match page and its composite cursor."""

        # Ask for one extra bucket so the API knows whether another catalog
        # page really exists without returning an empty final page.
        composite = {
            "size": page_size + 1,
            "sources": [{"sentenceKey": {"terms": {"field": "sentenceKey"}}}],
        }
        if after_key:
            composite["after"] = after_key
        response = self.client.search(
            index=self.config.catalog_alias_for(analysis_group),
            body={
                "size": 0,
                "track_total_hits": False,
                "query": {
                    "bool": {
                        "filter": catalog_eligibility_filters(),
                        "should": [
                            {"term": {"sentenceKey": source_key}},
                            {
                                "knn": {
                                    VECTOR_FIELD: catalog_vector_query(
                                        self.config,
                                        vector,
                                        threshold,
                                    )
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
                                    "_source": [
                                        "sentenceKey",
                                        "sentenceContent",
                                    ],
                                }
                            }
                        },
                    }
                },
            },
        )
        buckets = response["aggregations"]["matches"]["buckets"]
        page_buckets = buckets[:page_size]
        matches = []
        for bucket in page_buckets:
            hit = bucket["sample"]["hits"]["hits"][0]
            candidate = hit["_source"]
            candidate_key = candidate["sentenceKey"]
            if candidate_key == source_key:
                percentage = 100.0
                match_type = "exact"
            else:
                percentage = cosine_percentage(float(hit["_score"]))
                match_type = "similar"
            if percentage >= threshold:
                matches.append(
                    {
                        "sentenceKey": candidate_key,
                        "sentenceContent": candidate["sentenceContent"],
                        "similarityPercentage": percentage,
                        "matchType": match_type,
                    }
                )

        next_after_key = None
        if len(buckets) > page_size and page_buckets:
            next_after_key = page_buckets[-1]["key"]
        return {
            "matches": matches,
            "nextAfterKey": next_after_key,
        }

    def multi_search_catalog(
        self,
        bodies: list,
        analysis_group: str,
    ) -> list:
        """Send many catalog searches in one OpenSearch request."""

        catalog_alias = self.config.catalog_alias_for(analysis_group)
        request = []
        for body in bodies:
            request.extend(({"index": catalog_alias}, body))
        response = self.client.msearch(body=request)
        return response["responses"]

    def _eligible_occurrence_page(
        self,
        occurrence_filters: list,
        analysis_group: str,
        page_size: int,
        after_global_id: int | None,
        include_total: bool,
    ) -> dict:
        """Join occurrence locations to eligible catalog metadata in memory.

        OpenSearch cannot join two indexes. Occurrences are therefore read in
        global-ID order, and each batch of sentence keys is checked against the
        selected catalog. Only keys returned by the catalog are eligible.
        """

        collected = []
        total = 0
        scan_after = after_global_id
        exhausted = False
        while True:
            body = {
                "size": OCCURRENCE_SCAN_PAGE_SIZE,
                "track_total_hits": False,
                "_source": OCCURRENCE_SOURCE_FIELDS,
                "sort": [{"globalId": "asc"}],
                "query": {"bool": {"filter": occurrence_filters}},
            }
            if scan_after is not None:
                body["search_after"] = [scan_after]
            response = self.client.search(
                index=self.config.occurrence_alias,
                body=body,
            )
            hits = response["hits"]["hits"]
            if not hits:
                exhausted = True
                break

            catalog_records = self.catalog_records(
                [hit["_source"]["sentenceKey"] for hit in hits],
                analysis_group,
            )
            eligible = []
            for hit in hits:
                occurrence = hit["_source"]
                catalog = catalog_records.get(str(occurrence["sentenceKey"]))
                if catalog is not None:
                    eligible.append({**occurrence, **catalog})

            if include_total:
                total += len(eligible)
            remaining = (page_size + 1) - len(collected)
            if remaining > 0:
                collected.extend(eligible[:remaining])

            scan_after = int(hits[-1]["sort"][0])
            exhausted = len(hits) < OCCURRENCE_SCAN_PAGE_SIZE
            if exhausted:
                break
            if not include_total and len(collected) > page_size:
                break

        page = collected[:page_size]
        has_next_page = len(collected) > page_size or not exhausted
        next_after = None
        if has_next_page and page:
            next_after = int(page[-1]["globalId"])
        return {
            "sentences": page,
            "nextAfterGlobalId": next_after,
            "totalSentences": total if include_total else None,
        }

    def _hydrate_occurrences(
        self,
        occurrences: list,
        analysis_group: str,
    ) -> list:
        """Add catalog sentence text and eligibility fields to occurrences."""

        catalog_records = self.catalog_records(
            [occurrence["sentenceKey"] for occurrence in occurrences],
            analysis_group,
        )
        hydrated = []
        for occurrence in occurrences:
            catalog = catalog_records.get(str(occurrence["sentenceKey"]))
            if catalog is not None:
                hydrated.append({**occurrence, **catalog})
        return hydrated

    def application_occurrence_page(
        self,
        application_id: str,
        analysis_group: str,
        page_size: int,
        after_global_id: int | None,
        include_total: bool,
    ) -> dict:
        """Return one eligible application-sentence page in global-ID order."""

        canonical_group = self.config.canonical_analysis_group(analysis_group)
        return self._eligible_occurrence_page(
            [
                {"term": {"applicationId": application_id}},
                {"term": {"analysisGroup": canonical_group}},
            ],
            canonical_group,
            page_size,
            after_global_id,
            include_total,
        )

    def tsp_occurrence_page(
        self,
        tsp_id: str,
        analysis_group: str,
        page_size: int,
        after_global_id: int | None,
    ) -> dict:
        """Return one eligible document-sentence page in global-ID order.

        Input: tsp_id="TSP-10", analysis_group="Asylee", page_size=1000.
        Output: eligible source sentences and the cursor for the next page.
        """

        canonical_group = self.config.canonical_analysis_group(analysis_group)
        return self._eligible_occurrence_page(
            [
                {"term": {"tspId": tsp_id}},
                {"term": {"analysisGroup": canonical_group}},
            ],
            canonical_group,
            page_size,
            after_global_id,
            False,
        )

    def eligible_sentence_exists(
        self,
        sentence_key: str,
        analysis_group: str,
    ) -> bool:
        """Check that the key has an eligible occurrence in the analysis group."""

        canonical_group = self.config.canonical_analysis_group(analysis_group)
        if sentence_key not in self.catalog_records(
            [sentence_key],
            canonical_group,
        ):
            return False
        response = self.client.count(
            index=self.config.occurrence_alias,
            body={
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"sentenceKey": sentence_key}},
                            {"term": {"analysisGroup": canonical_group}},
                        ]
                    }
                }
            },
        )
        return int(response["count"]) > 0

    def occurrence_counts_by_key(
        self,
        sentence_keys: list,
        excluded_application_id: str,
        analysis_group: str,
    ) -> dict:
        """Count eligible outside-application occurrences for each key."""

        canonical_group = self.config.canonical_analysis_group(analysis_group)
        counts = {}
        unique_keys = sorted(set(sentence_keys))
        for offset in range(0, len(unique_keys), TERMS_BATCH_SIZE):
            batch = unique_keys[offset : offset + TERMS_BATCH_SIZE]
            response = self.client.search(
                index=self.config.occurrence_alias,
                body={
                    "size": 0,
                    "track_total_hits": False,
                    "query": {
                        "bool": {
                            "filter": [
                                {"terms": {"sentenceKey": batch}},
                                {"term": {"analysisGroup": canonical_group}},
                            ],
                            "must_not": [
                                {"term": {"applicationId": excluded_application_id}}
                            ],
                        }
                    },
                    "aggs": {
                        "keyCounts": {
                            "terms": {
                                "field": "sentenceKey",
                                "size": len(batch),
                            }
                        }
                    },
                },
            )
            for bucket in response["aggregations"]["keyCounts"]["buckets"]:
                counts[str(bucket["key"])] = int(bucket["doc_count"])
        return counts

    def occurrence_counts_by_application_and_key(
        self,
        sentence_keys: list,
        excluded_application_id: str,
        analysis_group: str,
    ) -> list:
        """Count eligible target occurrences for each application and key.

        Input: catalog keys matching sentences from application A1.
        Output: application/key/count rows for every application except A1.
        """

        canonical_group = self.config.canonical_analysis_group(analysis_group)
        unique_keys = sorted(set(sentence_keys))
        if not unique_keys:
            return []

        key_filters = []
        for offset in range(0, len(unique_keys), TERMS_BATCH_SIZE):
            batch = unique_keys[offset : offset + TERMS_BATCH_SIZE]
            key_filters.append({"terms": {"sentenceKey": batch}})

        counts = []
        after_key = None
        while True:
            composite = {
                "size": APPLICATION_BUCKET_PAGE_SIZE,
                "sources": [
                    {"applicationId": {"terms": {"field": "applicationId"}}},
                    {"sentenceKey": {"terms": {"field": "sentenceKey"}}},
                ],
            }
            if after_key:
                composite["after"] = after_key

            response = self.client.search(
                index=self.config.occurrence_alias,
                body={
                    "size": 0,
                    "track_total_hits": False,
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "bool": {
                                        "should": key_filters,
                                        "minimum_should_match": 1,
                                    }
                                },
                                {"term": {"analysisGroup": canonical_group}},
                            ],
                            "must_not": [
                                {"term": {"applicationId": excluded_application_id}}
                            ],
                        }
                    },
                    "aggs": {
                        "matches": {
                            "composite": composite,
                        }
                    },
                },
            )
            aggregation = response["aggregations"]["matches"]
            for bucket in aggregation["buckets"]:
                counts.append(
                    {
                        "applicationId": str(bucket["key"]["applicationId"]),
                        "sentenceKey": str(bucket["key"]["sentenceKey"]),
                        "occurrenceCount": int(bucket["doc_count"]),
                    }
                )
            after_key = aggregation.get("after_key")
            if not after_key:
                return counts

    def matching_occurrences_for_applications(
        self,
        sentence_keys: list,
        application_ids: list,
        excluded_application_id: str,
        analysis_group: str,
    ) -> list:
        """Return eligible occurrences for selected matching applications."""

        canonical_group = self.config.canonical_analysis_group(analysis_group)
        unique_keys = sorted(set(sentence_keys))
        unique_application_ids = sorted(set(application_ids))
        if not unique_keys or not unique_application_ids:
            return []

        key_filters = []
        for offset in range(0, len(unique_keys), TERMS_BATCH_SIZE):
            batch = unique_keys[offset : offset + TERMS_BATCH_SIZE]
            key_filters.append({"terms": {"sentenceKey": batch}})

        matches = []
        for offset in range(0, len(unique_application_ids), TERMS_BATCH_SIZE):
            application_batch = unique_application_ids[
                offset : offset + TERMS_BATCH_SIZE
            ]
            after_global_id = None
            while True:
                body = {
                    "size": MATCH_RESULT_BATCH_SIZE,
                    "track_total_hits": False,
                    "_source": OCCURRENCE_SOURCE_FIELDS,
                    "sort": [{"globalId": "asc"}],
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "bool": {
                                        "should": key_filters,
                                        "minimum_should_match": 1,
                                    }
                                },
                                {"terms": {"applicationId": application_batch}},
                                {"term": {"analysisGroup": canonical_group}},
                            ],
                            "must_not": [
                                {
                                    "term": {
                                        "applicationId": excluded_application_id
                                    }
                                }
                            ],
                        }
                    },
                }
                if after_global_id is not None:
                    body["search_after"] = [after_global_id]
                response = self.client.search(
                    index=self.config.occurrence_alias,
                    body=body,
                )
                hits = response["hits"]["hits"]
                matches.extend(
                    self._hydrate_occurrences(
                        [hit["_source"] for hit in hits],
                        canonical_group,
                    )
                )
                if len(hits) < MATCH_RESULT_BATCH_SIZE:
                    break
                after_global_id = int(hits[-1]["sort"][0])
        return matches

    def matching_occurrence_page(
        self,
        sentence_keys: list,
        excluded_application_id: str | None,
        analysis_group: str,
        page_size: int,
        after_global_id: int | None,
        include_total: bool,
        excluded_tsp_id: str | None = None,
    ) -> dict:
        """Return a page found by normal key filters, without vector search."""

        canonical_group = self.config.canonical_analysis_group(analysis_group)
        key_filters = []
        unique_keys = sorted(set(sentence_keys))
        for offset in range(0, len(unique_keys), TERMS_BATCH_SIZE):
            batch = unique_keys[offset : offset + TERMS_BATCH_SIZE]
            key_filters.append({"terms": {"sentenceKey": batch}})

        excluded_occurrences = []
        if excluded_application_id is not None:
            excluded_occurrences.append(
                {"term": {"applicationId": excluded_application_id}}
            )
        if excluded_tsp_id is not None:
            excluded_occurrences.append({"term": {"tspId": excluded_tsp_id}})

        body = {
            "size": page_size + 1,
            "track_total_hits": include_total,
            "_source": OCCURRENCE_SOURCE_FIELDS,
            "sort": [{"globalId": "asc"}],
            "query": {
                "bool": {
                    "filter": [
                        {
                            "bool": {
                                "should": key_filters,
                                "minimum_should_match": 1,
                            }
                        },
                        {"term": {"analysisGroup": canonical_group}},
                    ],
                    "must_not": excluded_occurrences,
                }
            },
        }
        if after_global_id is not None:
            body["search_after"] = [after_global_id]
        response = self.client.search(
            index=self.config.occurrence_alias,
            body=body,
        )
        hits = response["hits"]["hits"]
        page_hits = hits[:page_size]
        total = None
        if include_total:
            total = int(response["hits"]["total"]["value"])
        next_after = None
        if len(hits) > page_size and page_hits:
            next_after = int(page_hits[-1]["sort"][0])
        return {
            "matches": self._hydrate_occurrences(
                [hit["_source"] for hit in page_hits],
                canonical_group,
            ),
            "nextAfterGlobalId": next_after,
            "totalMatches": total,
        }

    def matching_occurrences(
        self,
        sentence_keys: list,
        excluded_application_id: str,
        analysis_group: str,
    ) -> list:
        """Return every matching occurrence using internal search-after pages."""

        matches = []
        after_global_id = None
        while True:
            page = self.matching_occurrence_page(
                sentence_keys,
                excluded_application_id,
                analysis_group,
                MATCH_RESULT_BATCH_SIZE,
                after_global_id,
                False,
            )
            matches.extend(page["matches"])
            after_global_id = page["nextAfterGlobalId"]
            if after_global_id is None:
                return matches

    def stats(self) -> dict:
        """Return occurrence and both catalog counts with cluster health."""

        return {
            "occurrenceDocuments": self._count(self.config.occurrence_alias),
            "asyleeCatalogDocuments": self._count(
                self.config.asylee_catalog_alias
            ),
            "refugeeCatalogDocuments": self._count(
                self.config.refugee_catalog_alias
            ),
            "clusterHealth": self.client.cluster.health()["status"],
        }

    def _count(self, index: str) -> int:
        """Count an index or return zero before it exists."""

        try:
            return int(self.client.count(index=index)["count"])
        except NotFoundError:
            return 0

    def preview(self, index: str, size: int) -> dict:
        """Return unfiltered documents and include catalog vectors."""

        body = {"size": size, "query": {"match_all": {}}}
        catalog_aliases = {
            self.config.asylee_catalog_alias,
            self.config.refugee_catalog_alias,
        }
        if index in catalog_aliases:
            body["_source"] = {"excludes": [VECTOR_FIELD]}
            body["docvalue_fields"] = [
                {
                    "field": VECTOR_FIELD,
                    "format": "binary",
                }
            ]
        response = self.client.search(index=index, body=body)
        documents = []
        for hit in response["hits"]["hits"]:
            document = {"id": hit["_id"], **hit["_source"]}
            if index in catalog_aliases:
                encoded_vector = hit.get("fields", {}).get(VECTOR_FIELD)
                if encoded_vector:
                    document[VECTOR_FIELD] = self._decode_binary_vector(
                        encoded_vector[0]
                    )
            documents.append(document)
        return {
            "count": len(documents),
            "documents": documents,
        }
