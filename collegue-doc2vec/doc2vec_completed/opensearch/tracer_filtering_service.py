from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.contracts import CatalogRecord, Classification
from doc2vec_completed.opensearch.opensearch_store import (
    SentenceCatalogOpenSearchStore,
    TracerOpenSearchStore,
)
from doc2vec_completed.opensearch.retry import retry_operation
from ta_utils.logger.CustomLogger import CustomLogger


class TracerFilteringService:
    """Classify unique sentence keys and cache the result in their catalog."""

    def __init__(
        self,
        sentence_catalog_opensearch_store: SentenceCatalogOpenSearchStore,
        tracer_opensearch_store: TracerOpenSearchStore,
        custom_logger: CustomLogger,
    ) -> None:
        self.sentence_catalog_opensearch_store = sentence_catalog_opensearch_store
        self.tracer_opensearch_store = tracer_opensearch_store
        self.logger = custom_logger

    def classify_with_tracer_and_save_to_sentence_catalog(
        self,
        catalog_alias: str,
        catalog_records: dict[str, CatalogRecord],
        analysis_group: str,
    ) -> dict[str, Classification]:
        """Reuse saved classifications and classify only unfinished keys.

        Input: two catalog records where key A has flags and key B does not.
        Output: classifications for both keys; only B searches the tracer index.
        """

        classifications: dict[str, Classification] = {}
        new_classifications: dict[str, Classification] = {}
        for sentence_key, catalog_record in catalog_records.items():
            classification = catalog_record["classification"]
            if classification is None:
                classification = retry_operation(
                    f"Classify sentence key {sentence_key}",
                    self.tracer_opensearch_store.search_classification_by_vector,
                    catalog_record["vector"],
                    analysis_group,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
                new_classifications[sentence_key] = classification
            classifications[sentence_key] = classification

        if new_classifications:
            retry_operation(
                "Save sentence classifications in the catalog",
                self.sentence_catalog_opensearch_store.update_catalog_classifications,
                catalog_alias,
                new_classifications,
                wait_seconds=constants.RETRY_SECONDS,
                max_attempts=1,
                custom_logger=self.logger,
            )
        return classifications
