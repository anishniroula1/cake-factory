from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock

from sentence_migration.config import MIGRATION_WORKERS, SOURCE_API_FIRST_PAGE
from sentence_migration.external_api import occurrence_from_api


@dataclass
class CatalogClaim:
    new_sources: list[dict]
    owned_futures: dict[str, Future]
    dependencies: list[Future]
    reused_count: int


class CatalogCoordinator:
    """Prevent workers from embedding the same sentence at the same time."""

    def __init__(self, opensearch_store, catalog_alias: str):
        self.opensearch = opensearch_store
        self.catalog_alias = catalog_alias
        self._lock = Lock()
        self._pending: dict[str, Future] = {}

    def claim(
        self, catalog_by_key: dict[str, dict], duplicate_count: int
    ) -> CatalogClaim:
        with self._lock:
            dependencies = {
                self._pending[key] for key in catalog_by_key if key in self._pending
            }
            candidates = [key for key in catalog_by_key if key not in self._pending]
            existing = self.opensearch.existing_catalog_keys(
                self.catalog_alias,
                candidates,
            )
            new_keys = [key for key in candidates if key not in existing]
            owned_futures = {key: Future() for key in new_keys}
            self._pending.update(owned_futures)
            return CatalogClaim(
                new_sources=[catalog_by_key[key] for key in new_keys],
                owned_futures=owned_futures,
                dependencies=list(dependencies),
                reused_count=duplicate_count + len(existing) + len(dependencies),
            )

    def complete(self, claim: CatalogClaim):
        with self._lock:
            for key, future in claim.owned_futures.items():
                future.set_result(True)
                self._pending.pop(key, None)

    def fail(self, claim: CatalogClaim, error: Exception):
        with self._lock:
            for key, future in claim.owned_futures.items():
                future.set_exception(error)
                self._pending.pop(key, None)


class SentenceMigrationService:
    def __init__(self, config, tsp_reader, external_api, opensearch_store):
        self.config = config
        self.tsp_reader = tsp_reader
        self.external_api = external_api
        self.opensearch = opensearch_store

    def _process_tsp_id(
        self,
        tsp_id: str,
        occurrence_alias: str,
        catalog_alias: str,
        catalog_coordinator: CatalogCoordinator,
    ) -> dict:
        totals = {
            "pagesRead": 0,
            "sentencesRead": 0,
            "occurrencesIndexed": 0,
            "catalogDocumentsIndexed": 0,
            "catalogDocumentsReused": 0,
            "sentenceKeysUpdated": 0,
        }
        seen_global_ids = set()
        page: int | str | None = SOURCE_API_FIRST_PAGE
        seen_pages = set()

        while page is not None:
            page_marker = f"{type(page).__name__}:{page}"
            if page_marker in seen_pages:
                raise RuntimeError(
                    f"Source API repeated nextPage {page!r} for TSP {tsp_id!r}"
                )
            seen_pages.add(page_marker)
            response_page = self.external_api.fetch_sentence_page(tsp_id, page)
            rows = response_page.items
            if not rows:
                page = response_page.next_page
                continue

            records = [occurrence_from_api(row, tsp_id) for row in rows]
            page_global_ids = {record.globalId for record in records}
            if len(page_global_ids) != len(records):
                raise ValueError(
                    f"Source API returned duplicate global IDs for TSP {tsp_id!r}, "
                    f"page {page}"
                )
            repeated_ids = page_global_ids & seen_global_ids
            if repeated_ids:
                repeated = min(repeated_ids)
                raise ValueError(
                    f"Source API repeated globalId {repeated} on multiple pages for "
                    f"TSP {tsp_id!r}"
                )
            seen_global_ids.update(page_global_ids)

            sources = [
                record.model_dump(
                    mode="json",
                    exclude={"sentenceContentVector"},
                )
                for record in records
            ]
            catalog_by_key = {}
            for source, record in zip(sources, records, strict=True):
                catalog_by_key.setdefault(
                    source["sentenceKey"],
                    self._catalog_source(source, record.sentenceContentVector),
                )
            duplicate_count = len(sources) - len(catalog_by_key)
            claim = catalog_coordinator.claim(catalog_by_key, duplicate_count)
            try:
                catalog_count = self.opensearch.bulk_index_catalog(
                    catalog_alias,
                    claim.new_sources,
                )
            except Exception as error:
                catalog_coordinator.fail(claim, error)
                raise
            catalog_coordinator.complete(claim)
            for dependency in claim.dependencies:
                dependency.result()

            occurrence_count = self.opensearch.bulk_index_occurrences(
                occurrence_alias,
                sources,
            )
            self.external_api.update_sentence_keys(tsp_id, records)

            totals["pagesRead"] += 1
            totals["sentencesRead"] += len(records)
            totals["occurrencesIndexed"] += occurrence_count
            totals["catalogDocumentsIndexed"] += catalog_count
            totals["catalogDocumentsReused"] += claim.reused_count
            totals["sentenceKeysUpdated"] += len(records)
            page = response_page.next_page

        if totals["occurrencesIndexed"]:
            self.opensearch.refresh_indices(occurrence_alias, catalog_alias)
        self.external_api.update_document_state(tsp_id)
        return {
            **totals,
            "documentsCompleted": 1,
        }

    def _complete_wave(self, pending_workers: list[Future]) -> dict:
        totals = {
            "pagesRead": 0,
            "sentencesRead": 0,
            "occurrencesIndexed": 0,
            "catalogDocumentsIndexed": 0,
            "catalogDocumentsReused": 0,
            "sentenceKeysUpdated": 0,
            "documentsCompleted": 0,
        }
        for pending in pending_workers:
            result = pending.result()
            for name in totals:
                totals[name] += result[name]
        return totals

    def migrate(
        self,
        object_key: str,
        occurrence_alias: str,
        catalog_alias: str,
    ) -> dict:
        """Run one complete paginated worker for each S3-listed TSP ID."""

        self.opensearch.require_existing_indices(occurrence_alias, catalog_alias)
        catalog_coordinator = CatalogCoordinator(self.opensearch, catalog_alias)
        totals = {
            "tspIdsRead": 0,
            "pagesRead": 0,
            "sentencesRead": 0,
            "occurrencesIndexed": 0,
            "catalogDocumentsIndexed": 0,
            "catalogDocumentsReused": 0,
            "sentenceKeysUpdated": 0,
            "documentsCompleted": 0,
            "completedWorkers": 0,
            "completedWaves": 0,
        }
        pending_workers = []

        with ThreadPoolExecutor(
            max_workers=MIGRATION_WORKERS,
            thread_name_prefix="tsp-migration",
        ) as executor:
            for tsp_id in self.tsp_reader.tsp_ids(object_key):
                pending_workers.append(
                    executor.submit(
                        self._process_tsp_id,
                        tsp_id,
                        occurrence_alias,
                        catalog_alias,
                        catalog_coordinator,
                    )
                )
                totals["tspIdsRead"] += 1

                if len(pending_workers) == MIGRATION_WORKERS:
                    result = self._complete_wave(pending_workers)
                    for name, value in result.items():
                        totals[name] += value
                    totals["completedWorkers"] += len(pending_workers)
                    totals["completedWaves"] += 1
                    pending_workers = []

            if pending_workers:
                result = self._complete_wave(pending_workers)
                for name, value in result.items():
                    totals[name] += value
                totals["completedWorkers"] += len(pending_workers)
                totals["completedWaves"] += 1

        return {
            **totals,
            "s3File": object_key,
            "s3Bucket": self.config.s3_bucket,
            "occurrenceAlias": occurrence_alias,
            "catalogAlias": catalog_alias,
            "workers": MIGRATION_WORKERS,
        }

    @staticmethod
    def _catalog_source(source: dict, vector: list[float]) -> dict:
        return {
            "sentenceContent": source["sentenceContent"],
            "sentenceContentVector": vector,
            "sentenceKey": source["sentenceKey"],
            "createdAt": source["createdAt"],
            "updatedAt": source["updatedAt"],
        }
