import base64
import binascii
import logging
import struct
import threading
import time

from opensearchpy import NotFoundError, RequestError, TransportError, helpers
from opensearchpy.helpers.errors import BulkIndexError

from sentence_search.config import Config
from sentence_search.open_search_client import OpenSearchClient
from sentence_search.search_utils import (
    cosine_percentage,
    minimum_opensearch_score,
)

SEMANTIC_TEXT_FIELD = "sentenceContent"
VECTOR_FIELD = "sentenceContentVector"
CATALOG_PAGE_SIZE = 1_000  # Catalog matches read from one aggregation page.
TERMS_BATCH_SIZE = 10_000  # Sentence keys sent in one terms filter.
VECTOR_BATCH_SIZE = 1_000  # Catalog vectors read in one OpenSearch request.
MATCH_RESULT_BATCH_SIZE = 5_000  # Occurrences read per internal search page.
APPLICATION_BUCKET_PAGE_SIZE = 1_000  # Application/key count buckets per page.
BULK_RETRY_ATTEMPTS = 10  # Temporary OpenSearch failures allowed per batch.
BULK_RETRY_SECONDS = 5  # Catalog shares this pause; occurrence uses it locally.
# Bedrock can return 408, 424, 429, 500, or 503. The Lasso or HTTP gateway can
# also return 502 or 504 while forwarding the same temporary failure.
RETRYABLE_STATUSES = {408, 424, 429, 500, 502, 503, 504}

logger = logging.getLogger(__name__)


def occurrence_index_definition(config: Config) -> dict:
    """Make the mapping for every sentence occurrence."""

    return {
        "settings": {
            "number_of_shards": config.index_shards,
            "number_of_replicas": config.index_replicas,
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "applicationId": {"type": "keyword"},
                "tspId": {"type": "keyword"},
                "sectionName": {"type": "keyword"},
                "globalId": {"type": "long"},
                "sentIdLocal": {"type": "long"},
                "sentenceContent": {"type": "text"},
                "isTracer": {"type": "boolean"},
                "isFormLanguage": {"type": "boolean"},
                "sentenceKey": {"type": "keyword"},
                "sourceType": {"type": "keyword"},
                "createdAt": {"type": "date"},
                "updatedAt": {"type": "date"},
                "analysisGroup": {"type": "keyword"},
            },
        },
    }


def catalog_index_definition(config: Config) -> dict:
    """Make the Titan-backed catalog using the trained Faiss IVF-PQ model."""

    return {
        "settings": {
            "index.knn": True,
            "index.default_pipeline": config.ingest_pipeline,
            "number_of_shards": config.index_shards,
            "number_of_replicas": config.index_replicas,
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "sentenceContent": {"type": "text"},
                "sentenceContentVector": {
                    "type": "knn_vector",
                    "model_id": config.ivf_model_id,
                },
                "sentenceKey": {"type": "keyword"},
                "createdAt": {"type": "date"},
                "updatedAt": {"type": "date"},
            },
        },
    }


class OpenSearchStore:
    def __init__(self, config: Config, client=None):
        """Create the store with an injected client or an AWS client."""

        self.config = config
        self.client = client or OpenSearchClient(config).create_client()
        self._catalog_backoff_lock = threading.Lock()
        self._catalog_cooldown_until = 0.0

    def wait_until_ready(self, attempts: int = 60):
        """Wait for OpenSearch during application startup."""

        for _ in range(attempts):
            if self.client.ping():
                return
            time.sleep(2)
        raise RuntimeError("OpenSearch did not become ready")

    def ensure_indices(self):
        """Create both physical indexes and aliases when missing."""

        self._ensure_index(
            self.config.occurrence_index,
            self.config.occurrence_alias,
            occurrence_index_definition(self.config),
        )
        self._ensure_index(
            self.config.catalog_index,
            self.config.catalog_alias,
            catalog_index_definition(self.config),
        )

    def _ensure_index(self, index: str, alias: str, definition: dict):
        """Create one physical index and point its write alias at it."""

        if not self.client.indices.exists(index=index):
            try:
                self.client.indices.create(index=index, body=definition)
            except RequestError:
                # The API can start while another process creates storage. Continue when
                # the other process successfully created this same index.
                if not self.client.indices.exists(index=index):
                    raise
        if not self.client.indices.exists_alias(name=alias):
            self.client.indices.put_alias(
                index=index,
                name=alias,
                body={"is_write_index": True},
            )

    def recreate_indices(self):
        """Delete and recreate both OpenSearch indexes."""

        self.delete_indices()
        self.ensure_indices()

    def delete_indices(self) -> dict:
        """Delete configured indexes and any indexes behind project aliases."""

        indexes = {
            self.config.occurrence_index,
            self.config.catalog_index,
        }
        for alias in (
            self.config.occurrence_alias,
            self.config.catalog_alias,
        ):
            if self.client.indices.exists_alias(name=alias):
                indexes.update(self.client.indices.get_alias(name=alias))

        deleted = []
        for index in sorted(indexes):
            if self.client.indices.exists(index=index):
                self.client.indices.delete(index=index)
                deleted.append(index)
        return {"deletedIndexes": deleted}

    def existing_catalog_keys(self, keys: list) -> set:
        """Return catalog document IDs that already exist."""

        existing = set()
        for offset in range(0, len(keys), 1_000):
            batch = keys[offset : offset + 1_000]
            response = self.client.mget(
                index=self.config.catalog_alias,
                body={"ids": batch},
                params={"_source": "false"},
            )
            for document in response["docs"]:
                if document.get("found"):
                    existing.add(str(document["_id"]))
        return existing

    def bulk_index_catalog(self, documents: list):
        """Index new catalog texts and retry temporary embedding failures."""

        if not documents:
            return 0
        actions = []
        for document in documents:
            actions.append(
                {
                    "_op_type": "index",
                    "_index": self.config.catalog_alias,
                    "_id": document["sentenceKey"],
                    "_source": document,
                }
            )
        self._bulk_with_retry(
            actions,
            "catalog",
            shared_catalog_cooldown=True,
        )
        return len(actions)

    def bulk_index_occurrences(self, documents: list):
        """Index sentence occurrences by globally unique sentence ID."""

        if not documents:
            return 0
        actions = []
        for document in documents:
            actions.append(
                {
                    "_op_type": "index",
                    "_index": self.config.occurrence_alias,
                    "_id": str(document["globalId"]),
                    "_source": document,
                }
            )
        self._bulk_with_retry(actions, "occurrence")
        return len(actions)

    def _bulk_with_retry(
        self,
        actions: list,
        label: str,
        shared_catalog_cooldown: bool = False,
    ):
        """Retry temporary bulk failures inside the worker handling the batch."""

        if not actions:
            return
        pending_actions = actions
        last_error = None
        for attempt in range(1, BULK_RETRY_ATTEMPTS + 1):
            if shared_catalog_cooldown:
                self._wait_for_catalog_cooldown()
            try:
                helpers.bulk(
                    self.client,
                    pending_actions,
                    chunk_size=self.config.seed_batch_size,
                    raise_on_error=True,
                )
                return
            except (BulkIndexError, TransportError) as error:
                last_error = error
                retry_actions = self._retryable_actions(
                    error,
                    pending_actions,
                )
                if retry_actions is None:
                    raise RuntimeError(
                        f"Non-retryable OpenSearch {label} bulk failure: {error}"
                    ) from error

                if attempt == BULK_RETRY_ATTEMPTS:
                    break
                pending_actions = retry_actions
                logger.warning(
                    "OpenSearch %s batch attempt %s/%s failed; retrying %s "
                    "document(s) after %s seconds",
                    label,
                    attempt,
                    BULK_RETRY_ATTEMPTS,
                    len(pending_actions),
                    BULK_RETRY_SECONDS,
                )
                if shared_catalog_cooldown:
                    # One Lasso or Titan failure pauses new catalog attempts
                    # from every worker. Requests already in flight continue.
                    self._start_catalog_cooldown()
                else:
                    # Occurrence indexing does not call Lasso or Titan, so
                    # only its own worker needs to pause before retrying.
                    time.sleep(BULK_RETRY_SECONDS)

        raise RuntimeError(
            f"OpenSearch {label} bulk failed after "
            f"{BULK_RETRY_ATTEMPTS} attempts: {last_error}"
        ) from last_error

    def _start_catalog_cooldown(self):
        """Set one shared retry time for all catalog workers."""

        with self._catalog_backoff_lock:
            self._catalog_cooldown_until = max(
                self._catalog_cooldown_until,
                time.monotonic() + BULK_RETRY_SECONDS,
            )

    def _wait_for_catalog_cooldown(self):
        """Wait until catalog workers may call Lasso and Titan again."""

        while True:
            with self._catalog_backoff_lock:
                remaining = self._catalog_cooldown_until - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(remaining)

    @staticmethod
    def _retryable_actions(
        error: BulkIndexError | TransportError,
        actions: list,
    ) -> list | None:
        """Return only failed actions that are safe to send again."""

        if isinstance(error, TransportError):
            status = getattr(error, "status_code", None)
            if status == "N/A":
                return actions
            try:
                status = int(status)
            except (TypeError, ValueError):
                return None
            return actions if status in RETRYABLE_STATUSES else None

        failed_ids = set()
        for item in error.errors:
            operation = next(iter(item.values()))
            status = operation.get("status")
            failed_id = operation.get("_id")
            try:
                status = int(status)
            except (TypeError, ValueError):
                return None
            if status not in RETRYABLE_STATUSES or failed_id is None:
                return None
            failed_ids.add(str(failed_id))

        if not failed_ids:
            return None
        retry_actions = [
            action for action in actions if str(action.get("_id")) in failed_ids
        ]
        retry_ids = {str(action.get("_id")) for action in retry_actions}
        return retry_actions if retry_ids == failed_ids else None

    def refresh_indices(self):
        """Make newly indexed catalog and occurrence records searchable."""

        self.client.indices.refresh(
            index=(f"{self.config.catalog_alias},{self.config.occurrence_alias}")
        )

    def catalog_vector(self, key: str) -> list:
        """Read one saved vector using fast binary doc values."""

        vectors = self.catalog_vectors([key])
        if key not in vectors:
            raise RuntimeError(f"Catalog vector does not exist for {key}")
        return vectors[key]

    def catalog_vectors(self, sentence_keys: list) -> dict:
        """Read saved vectors for sentence keys without loading `_source`.

        Input: two sentence keys already present in the catalog.
        Output: each sentence key mapped to its 512-number vector.
        """

        vectors = {}
        unique_keys = sorted(set(sentence_keys))
        for offset in range(0, len(unique_keys), VECTOR_BATCH_SIZE):
            batch = unique_keys[offset : offset + VECTOR_BATCH_SIZE]
            response = self.client.search(
                index=self.config.catalog_alias,
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
                    "query": {"ids": {"values": batch}},
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

    def _decode_binary_vector(self, encoded_vector: str) -> list:
        """Convert an OpenSearch binary vector into float values."""

        try:
            vector_bytes = base64.b64decode(
                encoded_vector,
                validate=True,
            )
        except (binascii.Error, ValueError) as error:
            raise RuntimeError("OpenSearch returned an invalid vector") from error

        expected_bytes = self.config.vector_dimension * 4
        if len(vector_bytes) != expected_bytes:
            raise RuntimeError(
                "OpenSearch returned a vector with "
                f"{len(vector_bytes)} bytes; expected {expected_bytes}"
            )
        return list(
            struct.unpack(
                f"<{self.config.vector_dimension}f",
                vector_bytes,
            )
        )

    def catalog_matches(
        self,
        source_key: str,
        vector: list,
        threshold: int,
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
            index=self.config.catalog_alias,
            body={
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
                                            "nprobes": self.config.ivf_nprobes,
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

    def multi_search_catalog(self, bodies: list) -> list:
        """Send many catalog searches in one OpenSearch request."""

        request = []
        for body in bodies:
            request.extend(({"index": self.config.catalog_alias}, body))
        response = self.client.msearch(body=request)
        return response["responses"]

    def application_occurrence_page(
        self,
        application_id: str,
        analysis_group: str,
        page_size: int,
        after_global_id: int | None,
        include_total: bool,
    ) -> dict:
        """Return one eligible application-sentence page in global-ID order."""

        body = {
            "size": page_size + 1,
            "track_total_hits": include_total,
            "_source": [
                "applicationId",
                "tspId",
                "sectionName",
                "globalId",
                "sentIdLocal",
                "sentenceContent",
                "sentenceKey",
                "sourceType",
                "analysisGroup",
            ],
            "sort": [{"globalId": "asc"}],
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"applicationId": application_id}},
                        {"term": {"analysisGroup": analysis_group}},
                        {"term": {"isTracer": False}},
                        {"term": {"isFormLanguage": False}},
                    ]
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
            "sentences": [hit["_source"] for hit in page_hits],
            "nextAfterGlobalId": next_after,
            "totalSentences": total,
        }

    def eligible_sentence_exists(
        self,
        sentence_key: str,
        analysis_group: str,
    ) -> bool:
        """Check that the key has an eligible occurrence in the analysis group."""

        response = self.client.count(
            index=self.config.occurrence_alias,
            body={
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"sentenceKey": sentence_key}},
                            {"term": {"analysisGroup": analysis_group}},
                            {"term": {"isTracer": False}},
                            {"term": {"isFormLanguage": False}},
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
                                {"term": {"analysisGroup": analysis_group}},
                                {"term": {"isTracer": False}},
                                {"term": {"isFormLanguage": False}},
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
                    {
                        "applicationId": {
                            "terms": {"field": "applicationId"}
                        }
                    },
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
                                {"term": {"analysisGroup": analysis_group}},
                                {"term": {"isTracer": False}},
                                {"term": {"isFormLanguage": False}},
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

        unique_keys = sorted(set(sentence_keys))
        unique_application_ids = sorted(set(application_ids))
        if not unique_keys or not unique_application_ids:
            return []

        key_filters = []
        for offset in range(0, len(unique_keys), TERMS_BATCH_SIZE):
            batch = unique_keys[offset : offset + TERMS_BATCH_SIZE]
            key_filters.append({"terms": {"sentenceKey": batch}})

        matches = []
        after_global_id = None
        while True:
            body = {
                "size": MATCH_RESULT_BATCH_SIZE,
                "track_total_hits": False,
                "_source": [
                    "applicationId",
                    "tspId",
                    "sectionName",
                    "globalId",
                    "sentIdLocal",
                    "sentenceContent",
                    "sentenceKey",
                    "sourceType",
                    "analysisGroup",
                ],
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
                            {"terms": {"applicationId": unique_application_ids}},
                            {"term": {"analysisGroup": analysis_group}},
                            {"term": {"isTracer": False}},
                            {"term": {"isFormLanguage": False}},
                        ],
                        "must_not": [
                            {"term": {"applicationId": excluded_application_id}}
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
            matches.extend(hit["_source"] for hit in hits)
            if len(hits) < MATCH_RESULT_BATCH_SIZE:
                return matches
            after_global_id = int(hits[-1]["sort"][0])

    def matching_occurrence_page(
        self,
        sentence_keys: list,
        excluded_application_id: str,
        analysis_group: str,
        page_size: int,
        after_global_id: int | None,
        include_total: bool,
    ) -> dict:
        """Return a page found by normal key filters, without vector search."""

        key_filters = []
        unique_keys = sorted(set(sentence_keys))
        for offset in range(0, len(unique_keys), TERMS_BATCH_SIZE):
            batch = unique_keys[offset : offset + TERMS_BATCH_SIZE]
            key_filters.append({"terms": {"sentenceKey": batch}})

        body = {
            "size": page_size + 1,
            "track_total_hits": include_total,
            "_source": [
                "applicationId",
                "tspId",
                "sectionName",
                "globalId",
                "sentIdLocal",
                "sentenceContent",
                "sentenceKey",
                "sourceType",
                "analysisGroup",
            ],
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
                        {"term": {"analysisGroup": analysis_group}},
                        {"term": {"isTracer": False}},
                        {"term": {"isFormLanguage": False}},
                    ],
                    "must_not": [{"term": {"applicationId": excluded_application_id}}],
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
            "matches": [hit["_source"] for hit in page_hits],
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
        """Return both OpenSearch document counts and cluster health."""

        return {
            "occurrenceDocuments": self._count(self.config.occurrence_alias),
            "catalogDocuments": self._count(self.config.catalog_alias),
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
        if index == self.config.catalog_alias:
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
            if index == self.config.catalog_alias:
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

    def delete_occurrences(
        self,
        application_id: str | None = None,
        tsp_id: str | None = None,
    ) -> int:
        """Delete occurrence documents for one application or TSP document."""

        filters = []
        if application_id is not None:
            filters.append({"term": {"applicationId": application_id}})
        if tsp_id is not None:
            filters.append({"term": {"tspId": tsp_id}})
        if not filters:
            raise ValueError("applicationId or tspId is required for deletion")
        response = self.client.delete_by_query(
            index=self.config.occurrence_alias,
            body={"query": {"bool": {"filter": filters}}},
            params={
                "conflicts": "proceed",
                "refresh": "true",
                "wait_for_completion": "true",
            },
        )
        return int(response.get("deleted", 0))

    def deletion_catalog_keys(
        self,
        application_id: str | None = None,
        tsp_id: str | None = None,
    ) -> list:
        """Return unique sentence keys before deleting occurrence documents."""

        filters = []
        if application_id is not None:
            filters.append({"term": {"applicationId": application_id}})
        if tsp_id is not None:
            filters.append({"term": {"tspId": tsp_id}})
        if not filters:
            raise ValueError("applicationId or tspId is required for deletion")

        keys = []
        after_key = None
        while True:
            composite = {
                "size": CATALOG_PAGE_SIZE,
                "sources": [{"sentenceKey": {"terms": {"field": "sentenceKey"}}}],
            }
            if after_key:
                composite["after"] = after_key
            response = self.client.search(
                index=self.config.occurrence_alias,
                body={
                    "size": 0,
                    "track_total_hits": False,
                    "query": {"bool": {"filter": filters}},
                    "aggs": {"keys": {"composite": composite}},
                },
            )
            result = response["aggregations"]["keys"]
            keys.extend(bucket["key"]["sentenceKey"] for bucket in result["buckets"])
            after_key = result.get("after_key")
            if not after_key or not result["buckets"]:
                break
        return keys

    def delete_unused_catalog_keys(self, keys: list) -> dict:
        """Delete unused vectors and return the keys removed with them."""

        deleted = 0
        deleted_keys = []
        for offset in range(0, len(keys), 1_000):
            batch = keys[offset : offset + 1_000]
            response = self.client.search(
                index=self.config.occurrence_alias,
                body={
                    "size": 0,
                    "track_total_hits": False,
                    "query": {"terms": {"sentenceKey": batch}},
                    "aggs": {
                        "usedKeys": {
                            "terms": {
                                "field": "sentenceKey",
                                "size": len(batch),
                            }
                        }
                    },
                },
            )
            used_keys = {
                bucket["key"]
                for bucket in response["aggregations"]["usedKeys"]["buckets"]
            }
            unused_keys = [key for key in batch if key not in used_keys]
            if not unused_keys:
                continue
            delete_response = self.client.delete_by_query(
                index=self.config.catalog_alias,
                body={"query": {"ids": {"values": unused_keys}}},
                params={
                    "conflicts": "proceed",
                    "refresh": "true",
                    "wait_for_completion": "true",
                },
            )
            deleted += int(delete_response.get("deleted", 0))
            deleted_keys.extend(unused_keys)
        return {"deleted": deleted, "sentenceKeys": deleted_keys}
