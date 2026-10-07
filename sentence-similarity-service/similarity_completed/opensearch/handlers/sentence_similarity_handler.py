from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from opensearchpy import OpenSearch
from sqlalchemy.orm import Session

from similarity_completed.opensearch.AwsSQS import AwsSQS
from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.contracts import (
    CatalogRecord,
    Classification,
    MatchTarget,
    ProcessResult,
    SentenceResult,
    TargetOccurrence,
    TspId,
)
from similarity_completed.opensearch.custom_logger import CustomLogger
from similarity_completed.opensearch.db_store import (
    SentencesVectorStore,
    TitanMatchesStore,
)
from similarity_completed.opensearch.errors import PermanentDataError
from similarity_completed.opensearch.models import SentencesVector
from similarity_completed.opensearch.opensearch_store import (
    OccurrenceStore,
    SentenceCatalogStore,
    TracerStore,
)
from similarity_completed.opensearch.retry import retry_operation
from similarity_completed.opensearch.utill.catalog import select_catalog_alias
from similarity_completed.opensearch.utill.filtering import (
    should_skip_sentence_catalog_search,
)


class SentenceSimilarityHandler:
    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch_client: OpenSearch,
        aws_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        """Keep everything required to process one document message."""

        self.config = config
        self.opensearch_client = opensearch_client
        self.aws_sqs = aws_sqs
        self.logger = custom_logger
        self.sentences_vector_store = SentencesVectorStore(
            config,
            session_factory,
        )
        self.titan_matches_store = TitanMatchesStore(
            config,
            session_factory,
        )
        self.occurrence_store = OccurrenceStore(config, opensearch_client)
        self.sentence_catalog_store = SentenceCatalogStore(
            config,
            opensearch_client,
        )
        self.tracer_store = TracerStore(config, opensearch_client)

    def process(self, tsp_id: TspId) -> ProcessResult:
        """Classify and match every sentence belonging to one document."""

        self.logger.info("Sentence Similarity processing started.")
        total = retry_operation(
            "Count source sentences",
            self.sentences_vector_store.count_sentences_by_tsp_id,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        if total == 0:
            raise PermanentDataError(
                "sentences_vector has no rows for the requested document"
            )
        self.logger.info(f"Found {total} source sentence(s).")

        processed = 0
        non_matchers = 0
        saved_relationships = 0
        after_global_id = 0
        batch_number = 0

        with ThreadPoolExecutor(
            max_workers=self.config.thread_worker_count,
            thread_name_prefix="sentence-similarity",
        ) as executor:
            while True:
                batch_number += 1
                rows = retry_operation(
                    f"Load source sentence batch {batch_number}",
                    self.sentences_vector_store.load_sentence_batch_by_tsp_id,
                    tsp_id,
                    after_global_id,
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )
                if not rows:
                    break

                catalog_alias = select_catalog_alias(
                    self.config,
                    rows[0].tsp.analysis_group,
                )
                source_records = retry_operation(
                    f"Load catalog vectors for sentence batch {batch_number}",
                    self.sentence_catalog_store.load_records_by_sentence_keys,
                    catalog_alias,
                    [row.sentence_key for row in rows],
                    wait_seconds=self.config.retry_seconds,
                    custom_logger=self.logger,
                )

                jobs = [
                    executor.submit(
                        self._process_sentence_with_retry,
                        row,
                        catalog_alias,
                        source_records[row.sentence_key],
                    )
                    for row in rows
                ]
                for job in jobs:
                    result = job.result()
                    processed += 1
                    non_matchers += int(result["is_non_matcher"])
                    saved_relationships += result["saved_relationships"]

                after_global_id = rows[-1].global_id
                self.logger.info(
                    "Completed source sentence batch "
                    f"{batch_number} with {len(rows)} sentence(s)."
                )

        retry_operation(
            "Send completed document to the next queue",
            self._send_to_api_transform,
            tsp_id,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Sentence Similarity completed {processed} sentence(s), including "
            f"{non_matchers} non-matcher(s) and {saved_relationships} saved "
            "relationships."
        )
        return {
            "tsp_id": tsp_id,
            "source_count": total,
            "processed_count": processed,
            "non_matcher_count": non_matchers,
            "saved_relationship_count": saved_relationships,
        }

    def _process_sentence_with_retry(
        self,
        source: SentencesVector,
        catalog_alias: str,
        source_record: CatalogRecord,
    ) -> SentenceResult:
        """Retry the complete classification and match flow for one sentence."""

        return retry_operation(
            f"Process source global ID {source.global_id}",
            self._process_sentence,
            source,
            catalog_alias,
            source_record,
            wait_seconds=self.config.retry_seconds,
            custom_logger=self.logger,
        )

    def _process_sentence(
        self,
        source: SentencesVector,
        catalog_alias: str,
        source_record: CatalogRecord,
    ) -> SentenceResult:
        """Classify one source, find eligible occurrences, and save matches."""

        stage = "source classification"
        try:
            classification = self._classify_sentence(
                catalog_alias,
                source_record,
                source.tsp.analysis_group,
            )
            self.sentences_vector_store.update_classification_by_global_ids(
                [source.global_id],
                classification,
            )

            if should_skip_sentence_catalog_search(classification):
                return {"is_non_matcher": True, "saved_relationships": 0}

            stage = "semantic catalog search"
            key_distances = (
                self.sentence_catalog_store.search_semantic_matches_by_vector(
                    catalog_alias,
                    source.sentence_key,
                    source_record["vector"],
                )
            )

            stage = "target occurrence expansion"
            targets = self._find_eligible_match_targets(
                source,
                catalog_alias,
                key_distances,
                source_record,
            )

            stage = "bidirectional Titan match write"
            saved = self.titan_matches_store.save_bidirectional_matches(
                source,
                targets,
            )
            return {
                "is_non_matcher": False,
                "saved_relationships": saved,
            }
        except Exception as error:
            self.logger.error(
                f"Source global ID {source.global_id} failed during {stage}: "
                f"{str(error)[:500]}"
            )
            raise

    def _classify_sentence(
        self,
        catalog_alias: str,
        record: CatalogRecord,
        analysis_group: str,
    ) -> Classification:
        """Reuse a catalog decision or calculate and save it once."""

        saved = record["classification"]
        # The catalog keeps only the three flags. A saved non-matcher must be
        # searched again so sentences_vector also receives its reference ID
        # and score. Allowed sentences do not need those two database values.
        if saved is not None and not should_skip_sentence_catalog_search(saved):
            return saved
        classification = self.tracer_store.classify_sentence_by_vector(
            record["vector"],
            analysis_group,
        )
        self.sentence_catalog_store.update_classification_by_sentence_key(
            catalog_alias,
            record["sentence_key"],
            classification,
        )
        record["classification"] = classification
        return classification

    def _find_eligible_match_targets(
        self,
        source: SentencesVector,
        catalog_alias: str,
        key_distances: dict[str, float],
        source_record: CatalogRecord,
    ) -> dict[str, MatchTarget]:
        """Expand matching keys and remove every ineligible target occurrence."""

        occurrences = (
            self.occurrence_store.search_matching_occurrences_outside_application(
                key_distances,
                source.tsp.analysis_group,
                source.tsp.application_id,
            )
        )
        if not occurrences:
            return {}

        occurrences_by_key: dict[str, list[TargetOccurrence]] = defaultdict(list)
        for occurrence in occurrences:
            occurrences_by_key[occurrence["sentence_key"]].append(occurrence)

        records = self.sentence_catalog_store.load_records_by_sentence_keys(
            catalog_alias,
            list(occurrences_by_key),
        )
        records[source_record["sentence_key"]] = source_record

        missing_keys = sorted(set(occurrences_by_key) - set(records))
        if missing_keys:
            raise PermanentDataError(
                f"catalog vector is missing for sentence key {missing_keys[0]}"
            )

        targets: dict[str, MatchTarget] = {}
        for sentence_key, matching_occurrences in occurrences_by_key.items():
            record = records[sentence_key]
            classification = self._classify_sentence(
                catalog_alias,
                record,
                source.tsp.analysis_group,
            )
            self.sentences_vector_store.update_classification_by_global_ids(
                [item["global_id"] for item in matching_occurrences],
                classification,
            )
            if should_skip_sentence_catalog_search(classification):
                continue

            for occurrence in matching_occurrences:
                targets[str(occurrence["global_id"])] = {
                    "tsp_id": occurrence["tsp_id"],
                    "application_id": occurrence["application_id"],
                    "distance": occurrence["distance"],
                }
        return targets

    def _send_to_api_transform(self, tsp_id: TspId) -> None:
        """Send the completed document to API Transform."""

        sent = self.aws_sqs.send_sqs_message(
            send_queue_url=self.config.api_transform_queue_url,
            message_body={"tsp_id": tsp_id},
            is_fifo=self.config.api_transform_queue_url.endswith(".fifo"),
        )
        if not sent:
            raise RuntimeError("failed to send the completed API Transform message")
