import csv
import re
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile

from sentence_search.opensearch_store import (
    MATCH_RESULT_BATCH_SIZE,
    OpenSearchStore,
)
from sentence_search.sentence_summary_service import SentenceSummaryService

SOURCE_PAGE_SIZE = 1000
CSV_FIELDS = [
    "source_application_id",
    "source_tsp_id",
    "source_global_id",
    "source_sent_id_local",
    "source_sentence",
    "target_application_id",
    "target_tsp_id",
    "target_global_id",
    "target_sent_id_local",
    "target_sentence",
    "distance",
    "match_percentage",
    "opensearch_score",
    "match_type",
    "analysis_group",
    "requested_threshold",
]


@dataclass(frozen=True)
class MatchCsvExport:
    path: Path
    file_name: str
    match_count: int


class MatchCsvExportService:
    """Create complete sentence-match CSV files from read-only searches."""

    def __init__(
        self,
        opensearch: OpenSearchStore,
        sentence_summary: SentenceSummaryService,
    ):
        self.opensearch = opensearch
        self.sentence_summary = sentence_summary

    def create_application_csv(
        self,
        application_id: str,
        analysis_group: str,
        threshold: int,
    ) -> MatchCsvExport:
        """Export every eligible match for one application.

        Input: application_id="123", analysis_group="Asylee", threshold=90.
        Output: application_id-123.csv containing all source-target pairs.
        """

        self._validate_threshold(threshold)
        source_sentences = self._application_sentences(
            application_id,
            analysis_group,
        )
        if not source_sentences:
            raise ValueError(
                "applicationId has no eligible sentences for this analysisGroup"
            )
        return self._create_csv(
            source_sentences,
            application_id,
            analysis_group,
            threshold,
            f"application_id-{self._safe_file_value(application_id)}.csv",
        )

    def create_tsp_csv(
        self,
        tsp_id: str,
        analysis_group: str,
        threshold: int,
    ) -> MatchCsvExport:
        """Export every eligible match for one TSP document.

        Input: tsp_id="456", analysis_group="Asylee", threshold=90.
        Output: tsp_id-456.csv containing all source-target pairs.
        """

        self._validate_threshold(threshold)
        source_sentences = self._tsp_sentences(tsp_id, analysis_group)
        if not source_sentences:
            raise ValueError("tspId has no eligible sentences for this analysisGroup")
        source_application_ids = {
            str(sentence["applicationId"]) for sentence in source_sentences
        }
        if len(source_application_ids) != 1:
            raise RuntimeError("tspId belongs to more than one application")
        return self._create_csv(
            source_sentences,
            source_application_ids.pop(),
            analysis_group,
            threshold,
            f"tsp_id-{self._safe_file_value(tsp_id)}.csv",
        )

    def _create_csv(
        self,
        source_sentences: list,
        source_application_id: str,
        analysis_group: str,
        threshold: int,
        file_name: str,
    ) -> MatchCsvExport:
        """Search all unique source keys and write every occurrence pair."""

        source_sentences_by_key = self._sentences_by_key(source_sentences)
        match_details = self.sentence_summary.matching_key_details(
            sorted(source_sentences_by_key),
            analysis_group,
            threshold,
        )
        relations_by_target = self._relations_by_target(match_details)

        temporary_path = None
        try:
            with NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="",
                suffix=".csv",
                delete=False,
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)
                writer = csv.DictWriter(temporary_file, fieldnames=CSV_FIELDS)
                writer.writeheader()
                match_count = self._write_match_rows(
                    writer,
                    source_sentences_by_key,
                    relations_by_target,
                    source_application_id,
                    analysis_group,
                    threshold,
                )
        except Exception:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
            raise

        return MatchCsvExport(temporary_path, file_name, match_count)

    def _write_match_rows(
        self,
        writer: csv.DictWriter,
        source_sentences_by_key: dict,
        relations_by_target: dict,
        source_application_id: str,
        analysis_group: str,
        threshold: int,
    ) -> int:
        """Page target occurrences and write each source-target pair."""

        target_keys = sorted(relations_by_target)
        if not target_keys:
            return 0

        match_count = 0
        after_global_id = None
        while True:
            page = self.opensearch.matching_occurrence_page(
                target_keys,
                source_application_id,
                analysis_group,
                MATCH_RESULT_BATCH_SIZE,
                after_global_id,
                False,
            )
            for target in page["matches"]:
                for relation in relations_by_target.get(
                    target["sentenceKey"],
                    [],
                ):
                    for source in source_sentences_by_key[relation["sourceKey"]]:
                        writer.writerow(
                            self._csv_row(
                                source,
                                target,
                                relation,
                                analysis_group,
                                threshold,
                            )
                        )
                        match_count += 1

            after_global_id = page["nextAfterGlobalId"]
            if after_global_id is None:
                return match_count

    def _application_sentences(
        self,
        application_id: str,
        analysis_group: str,
    ) -> list:
        """Load every eligible source sentence for one application."""

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

    def _tsp_sentences(self, tsp_id: str, analysis_group: str) -> list:
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
        """Group source occurrences so each vector is searched once."""

        grouped = {}
        for sentence in sentences:
            grouped.setdefault(sentence["sentenceKey"], []).append(sentence)
        return grouped

    @staticmethod
    def _relations_by_target(match_details: dict) -> dict:
        """Map each matching target key back to its source sentence keys."""

        relations = {}
        for source_key in sorted(match_details):
            for target_key, details in match_details[source_key].items():
                relations.setdefault(target_key, []).append(
                    {"sourceKey": source_key, **details}
                )
        return relations

    @staticmethod
    def _csv_row(
        source: dict,
        target: dict,
        relation: dict,
        analysis_group: str,
        threshold: int,
    ) -> dict:
        """Create one CSV row for one source and target sentence pair."""

        return {
            "source_application_id": source.get("applicationId", ""),
            "source_tsp_id": source.get("tspId", ""),
            "source_global_id": source.get("globalId", ""),
            "source_sent_id_local": source.get("sentIdLocal", ""),
            "source_sentence": source.get("sentenceContent", ""),
            "target_application_id": target.get("applicationId", ""),
            "target_tsp_id": target.get("tspId", ""),
            "target_global_id": target.get("globalId", ""),
            "target_sent_id_local": target.get("sentIdLocal", ""),
            "target_sentence": target.get("sentenceContent", ""),
            "distance": relation["cosineDistance"],
            "match_percentage": relation["matchPercentage"],
            "opensearch_score": relation["openSearchScore"],
            "match_type": relation["matchType"],
            "analysis_group": analysis_group,
            "requested_threshold": threshold,
        }

    @staticmethod
    def _validate_threshold(threshold: int) -> None:
        """Require a cosine threshold between 1 and 100 percent."""

        if not 1 <= threshold <= 100:
            raise ValueError("threshold must be between 1 and 100")

    @staticmethod
    def _safe_file_value(value: str) -> str:
        """Keep an identifier safe for a CSV download name."""

        safe_value = re.sub(r"[^A-Za-z0-9._-]", "_", str(value))
        return safe_value or "unknown"
