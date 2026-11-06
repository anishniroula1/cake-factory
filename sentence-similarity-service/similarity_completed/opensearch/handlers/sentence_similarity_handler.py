import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from opensearchpy import OpenSearch
from sqlalchemy.orm import Session

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.contracts import (
    CatalogRecord,
    MatchTarget,
    ProcessResult,
    SentenceResult,
    TitanMatchRelationship,
    TspId,
)
from similarity_completed.opensearch.db_store import SentenceVectorDBStore, TitanMatchesDBStore
from similarity_completed.opensearch.errors import QueueSendError
from similarity_completed.opensearch.opensearch_store import OccurrenceOpenSearchStore, SentenceCatalogOpenSearchStore
from similarity_completed.opensearch.retry import retry_operation, safe_error_summary
from similarity_completed.opensearch.utill.catalog import select_catalog_alias
from ta_db_connection.atatables import SentencesVector
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class SentenceSimilarityHandler:
    """Search eligible document sentences and save their semantic relationships."""

    def __init__(
        self,
        config: Config,
        session_factory: Callable[[], Session],
        opensearch_client: OpenSearch,
        aws_sqs: AwsSQS,
        custom_logger: CustomLogger,
    ) -> None:
        self.config = config
        self.opensearch_client = opensearch_client
        self.aws_sqs = aws_sqs
        self.logger = custom_logger
        self.sentence_vector_db_store = SentenceVectorDBStore(session_factory)
        self.titan_matches_db_store = TitanMatchesDBStore(session_factory, custom_logger)
        self.occurrence_open_search_store = OccurrenceOpenSearchStore(opensearch_client)
        self.sentence_catalog_open_search_store = SentenceCatalogOpenSearchStore(opensearch_client)

    def process_document_sentences(self, tsp_id: TspId) -> ProcessResult | None:
        """Process every eligible source sentence associated with one document.
        Doc2Vec has already calculated word counts and tracer classifications,
        so this flow loads only rows that can create semantic relationships.

        Input: tsp_id=8205
        Output: {"tsp_id": 8205, "source_count": 40, "processed_count": 30,
        "matched_sentence_count": 20, "saved_relationship_count": 125}. In
        this example, 10 source rows were excluded before worker processing.

        The method returns None when occurrence or catalog data is not ready.
        """

        started_at = perf_counter()
        self.logger.info("Sentence Similarity processing started.")
        if not self._wait_for_document_occurrences(tsp_id):
            return None
        source_sentence_records = self._load_all_document_sentences(tsp_id)
        eligible_source_sentences = self._select_eligible_source_sentences(
            source_sentence_records
        )
        total_source_sentence_count = len(source_sentence_records)

        processed_sentence_count = 0
        matched_sentence_count = 0
        collected_match_relationships: list[TitanMatchRelationship] = []

        if eligible_source_sentences:
            catalog_alias = select_catalog_alias(
                eligible_source_sentences[0].tsp.analysis_group
            )
            source_catalog_records = retry_operation(
                "Load source catalog vectors",
                self.sentence_catalog_open_search_store.load_catalog_records_by_sentence_keys,
                catalog_alias,
                [record.sentence_key for record in eligible_source_sentences],
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=5,
                custom_logger=self.logger,
            )

            self.logger.info(
                f"Processing {len(eligible_source_sentences)} eligible source "
                "sentence(s); "
                f"starting_global_id={eligible_source_sentences[0].global_id}, "
                f"ending_global_id={eligible_source_sentences[-1].global_id}."
            )
            with ThreadPoolExecutor(
                max_workers=constants.THREAD_WORKER_COUNT,
                thread_name_prefix="sentence-similarity",
            ) as executor:
                sentence_job_batches = []
                all_sentence_jobs = []
                for batch_number, offset in enumerate(
                    range(
                        0,
                        len(eligible_source_sentences),
                        constants.SOURCE_SENTENCE_PROGRESS_BATCH_SIZE,
                    ),
                    start=1,
                ):
                    source_sentence_batch = eligible_source_sentences[
                        offset : offset
                        + constants.SOURCE_SENTENCE_PROGRESS_BATCH_SIZE
                    ]
                    starting_global_id = source_sentence_batch[0].global_id
                    ending_global_id = source_sentence_batch[-1].global_id
                    self.logger.info(
                        f"Starting source sentence thread batch {batch_number} "
                        f"with {len(source_sentence_batch)} sentence(s); "
                        f"starting_global_id={starting_global_id}, "
                        f"ending_global_id={ending_global_id}."
                    )
                    sentence_jobs = [
                        executor.submit(
                            self._process_source_sentence_with_error_logging,
                            source_sentence,
                            catalog_alias,
                            source_catalog_records[source_sentence.sentence_key],
                        )
                        for source_sentence in source_sentence_batch
                    ]
                    all_sentence_jobs.extend(sentence_jobs)
                    sentence_job_batches.append(
                        (
                            batch_number,
                            starting_global_id,
                            ending_global_id,
                            sentence_jobs,
                        )
                    )

                for (
                    batch_number,
                    starting_global_id,
                    ending_global_id,
                    sentence_jobs,
                ) in sentence_job_batches:
                    for sentence_job in sentence_jobs:
                        sentence_result = sentence_job.result()
                        if sentence_result is None:
                            for unfinished_job in all_sentence_jobs:
                                unfinished_job.cancel()
                            return None

                        match_relationships = sentence_result["match_relationships"]
                        processed_sentence_count += 1
                        matched_sentence_count += int(bool(match_relationships))
                        collected_match_relationships.extend(match_relationships)
                    self.logger.info(
                        f"Completed source sentence thread batch {batch_number}; "
                        f"starting_global_id={starting_global_id}, "
                        f"ending_global_id={ending_global_id}."
                    )

        self.logger.info(
            f"Starting titan_matches database writes for "
            f"{len(collected_match_relationships)} collected relationship(s)."
        )
        titan_match_write_started_at = perf_counter()
        saved_relationship_count = self.titan_matches_db_store.save_bidirectional_match_relationships(
            collected_match_relationships
        )
        self.logger.info(
            f"titan_matches database writes completed in "
            f"{perf_counter() - titan_match_write_started_at:.2f} seconds and "
            f"saved {saved_relationship_count} unique relationship(s)."
        )

        retry_operation(
            "Send completed document to the next queue",
            self._send_similarity_result_to_next_queue,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        self.logger.info(
            f"Sentence Similarity completed {processed_sentence_count} sentence(s): "
            f"{matched_sentence_count} sentence(s) had matches, "
            f"and {saved_relationship_count} relationship(s) were saved."
        )
        self.logger.info(f"Sentence Similarity took {perf_counter() - started_at:.2f} seconds to complete.")
        return {
            "tsp_id": tsp_id,
            "source_count": total_source_sentence_count,
            "processed_count": processed_sentence_count,
            "matched_sentence_count": matched_sentence_count,
            "saved_relationship_count": saved_relationship_count,
        }

    def _load_all_document_sentences(
        self,
        tsp_id: TspId,
    ) -> list[SentencesVector]:
        """Load every document sentence before starting worker threads.

        Input: tsp_id=8205
        Output: all sentence rows in global-ID order. PostgreSQL still returns
        bounded pages so unusually large documents do not use one huge query.
        """

        source_sentence_records: list[SentencesVector] = []
        after_global_id = 0
        page_number = 0
        while True:
            page_number += 1
            sentence_batch = retry_operation(
                f"Load source sentence batch {page_number}",
                self.sentence_vector_db_store.load_document_sentence_batch,
                tsp_id,
                after_global_id,
                wait_seconds=constants.RETRY_SECONDS,
                custom_logger=self.logger,
            )
            if not sentence_batch:
                return source_sentence_records

            source_sentence_records.extend(sentence_batch)
            after_global_id = sentence_batch[-1].global_id
            if len(sentence_batch) < constants.DATABASE_SENTENCE_LOAD_BATCH_SIZE:
                return source_sentence_records

    def _select_eligible_source_sentences(
        self,
        source_sentence_records: list[SentencesVector],
    ) -> list[SentencesVector]:
        """Select matchable rows and log why other rows are skipped.

        A classification count is the number of unique rows having at least
        one classification flag. Individual flag counts can overlap because a
        sentence may be both non-matcher and form language.
        """

        short_sentence_count = sum(
            (record.word_count or 0) < constants.MINIMUM_MATCH_WORD_COUNT
            for record in source_sentence_records
        )
        non_matcher_count = sum(
            bool(record.is_non_matcher) for record in source_sentence_records
        )
        tracer_count = sum(bool(record.is_tracer) for record in source_sentence_records)
        form_language_count = sum(
            bool(record.is_form_language) for record in source_sentence_records
        )
        classification_skip_count = sum(
            bool(record.is_non_matcher or record.is_tracer or record.is_form_language)
            for record in source_sentence_records
        )
        eligible_source_sentences = [
            record
            for record in source_sentence_records
            if (record.word_count or 0) >= constants.MINIMUM_MATCH_WORD_COUNT
            and not record.is_non_matcher
            and not record.is_tracer
            and not record.is_form_language
        ]
        skipped_sentence_count = len(source_sentence_records) - len(
            eligible_source_sentences
        )
        self.logger.info(
            f"Loaded {len(source_sentence_records)} source sentence(s). "
            f"Eligible: {len(eligible_source_sentences)}. Skipped: "
            f"{skipped_sentence_count}. Short sentences: "
            f"{short_sentence_count}. Classification skips: "
            f"{classification_skip_count} (non-matcher: {non_matcher_count}, "
            f"tracer: {tracer_count}, form language: {form_language_count})."
        )
        return eligible_source_sentences

    def _process_source_sentence(
        self, source_sentence: SentencesVector, catalog_alias: str, source_catalog_record: CatalogRecord
    ) -> SentenceResult | None:
        """Search one eligible source sentence and prepare its relationships.

        Input: source_sentence.global_id=39205,
        catalog_alias="sentence_semantic_catalog_asylee", and a source_catalog_record
        containing a 512-value vector.
        Output: {"match_relationships": [{...}]}

        The method returns None when a required target catalog record is missing.
        """

        sentence_key_distances = retry_operation(
            f"Search sentence catalog for source global_id={source_sentence.global_id}",
            self.sentence_catalog_open_search_store.search_matching_sentence_keys_by_vector,
            catalog_alias,
            source_sentence.sentence_key,
            source_catalog_record["vector"],
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )

        target_occurrences = retry_operation(
            f"Expand matching occurrences for source global_id={source_sentence.global_id}",
            self.occurrence_open_search_store.search_target_occurrences_outside_source_application,
            sentence_key_distances,
            source_sentence.tsp.analysis_group,
            source_sentence.tsp.application_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )
        eligible_target_occurrences: dict[str, MatchTarget] = {
            str(target["global_id"]): {
                "tsp_id": target["tsp_id"],
                "application_id": target["application_id"],
                "distance": target["distance"],
            }
            for target in target_occurrences
        }
        relationship_count = len(eligible_target_occurrences)
        if relationship_count > constants.RELATIONSHIP_LOG_THRESHOLD:
            self.logger.warning(
                f"Source global_id={source_sentence.global_id} has "
                f"{relationship_count} eligible sentence relationship(s)."
            )

        return {
            "match_relationships": self._build_titan_match_relationships(
                source_sentence, eligible_target_occurrences
            ),
        }

    def _process_source_sentence_with_error_logging(
        self,
        source_sentence: SentencesVector,
        catalog_alias: str,
        source_catalog_record: CatalogRecord,
    ) -> SentenceResult | None:
        """Process one thread job and identify its source row when it fails."""

        try:
            return self._process_source_sentence(
                source_sentence,
                catalog_alias,
                source_catalog_record,
            )
        except Exception as error:
            self.logger.error(
                "Source sentence thread failed for "
                f"global_id={source_sentence.global_id}: "
                f"{safe_error_summary(error)}"
            )
            raise

    @staticmethod
    def _build_titan_match_relationships(
        source_sentence: SentencesVector,
        eligible_target_occurrences: dict[str, MatchTarget],
    ) -> list[TitanMatchRelationship]:
        """Convert one sentence's matches into relationships grouped later for database writes."""

        return [
            {
                "source_global_id": source_sentence.global_id,
                "source_tsp_id": source_sentence.tsp_id,
                "source_application_id": source_sentence.tsp.application_id,
                "target_global_id": int(target_global_id),
                "target_tsp_id": target["tsp_id"],
                "target_application_id": target["application_id"],
                "distance": target["distance"],
            }
            for target_global_id, target in eligible_target_occurrences.items()
        ]

    def _wait_for_document_occurrences(self, tsp_id: TspId) -> bool:
        """Wait once for Doc2Vec occurrence records to become searchable.
        OpenSearch refreshes asynchronously, so a message can arrive before its
        occurrence records become visible.

        Input: tsp_id=8205
        Output: True when at least one occurrence is searchable, or False when
        both readiness checks return zero records.
        """

        self.logger.info("Checking whether sentence occurrence records are searchable.")
        if self._document_occurrences_are_searchable(tsp_id):
            self.logger.info("Sentence occurrence records are searchable. Continuing processing.")
            return True

        # The prior service can publish its SQS message just before an
        # OpenSearch refresh becomes visible. Waiting 30 seconds gives the
        # index time to refresh and gives both services a short breathing point.
        self.logger.warning(
            "Sentence occurrence records are not searchable yet. "
            f"Waiting {constants.OCCURRENCE_READINESS_WAIT_SECONDS} seconds before checking once more."
        )
        time.sleep(constants.OCCURRENCE_READINESS_WAIT_SECONDS)

        if self._document_occurrences_are_searchable(tsp_id):
            self.logger.info("Sentence occurrence records became searchable. Continuing processing.")
            return True

        self.logger.error(
            "Sentence occurrence records are still not searchable after the "
            f"{constants.OCCURRENCE_READINESS_WAIT_SECONDS}-second wait."
        )
        return False

    def _document_occurrences_are_searchable(self, tsp_id: TspId) -> bool:
        """Check occurrence-index readiness with transient-error retries.

        Input: tsp_id=8205
        Output: True when OpenSearch contains at least one occurrence; otherwise False.
        """

        return retry_operation(
            "Check sentence occurrence readiness",
            self.occurrence_open_search_store.has_document_occurrences,
            tsp_id,
            wait_seconds=constants.RETRY_SECONDS,
            custom_logger=self.logger,
        )

    def _send_similarity_result_to_next_queue(self, tsp_id: TspId) -> None:
        """Publish the completed document to the downstream queue. The outgoing
        message identifies P2 as the source and Sentence Similarity as the sender.

        Input: tsp_id=8205

        The queue receives {"source": "P2", "tsp_id": "8205",
        "sent_by": "sentence_similarity"}.

        Raises:
            QueueSendError: If SQS does not accept the message.
        """

        message_sent = self.aws_sqs.send_sqs_message(
            send_queue_url=self.config.send_sqs_endpoint,
            message_body={"source": "P2", "tsp_id": str(tsp_id), "sent_by": "sentence-similarity"},
            is_fifo=self.config.send_sqs_endpoint.endswith(".fifo"),
        )
        if not message_sent:
            raise QueueSendError("failed to send the completed message to the next queue")
