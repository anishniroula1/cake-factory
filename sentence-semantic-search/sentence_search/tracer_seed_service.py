import csv
from io import StringIO

from sentence_search.config import Config
from sentence_search.models import TracerSentence
from sentence_search.opensearch_store import OpenSearchStore

REQUIRED_COLUMNS = {
    "tracerId",
    "sentenceContent",
    "analysisGroup",
    "isNonMatcher",
    "isFormLanguage",
}

HEADER_ALIASES = {
    "tracer_id": "tracerId",
    "tracer_sentence_id": "tracerId",
    "tracerSentenceId": "tracerId",
    "sentence_content": "sentenceContent",
    "analysis_group": "analysisGroup",
    "is_non_matcher": "isNonMatcher",
    "is_form_language": "isFormLanguage",
    "created_at": "createdAt",
    "updated_at": "updatedAt",
}
OPTIONAL_COLUMNS = {"createdAt", "updatedAt"}


def _as_bool(value: str) -> bool:
    """Read a CSV boolean such as true, false, 1, 0, yes, or no."""

    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


class TracerSeedService:
    def __init__(self, config: Config, opensearch: OpenSearchStore):
        """Save the settings and OpenSearch store used by tracer uploads."""

        self.config = config
        self.opensearch = opensearch

    def seed(self, csv_bytes: bytes) -> dict:
        """Validate one uploaded CSV and index all tracer sentences.

        Input: CSV bytes containing tracer_id, sentence_content,
        analysis_group, is_non_matcher, and is_form_language.
        Output: upload and indexing counts. The source never contains a vector;
        the ingest pipeline attached to the tracer alias creates it.
        """

        if not csv_bytes:
            raise ValueError("The uploaded CSV is empty")
        try:
            csv_text = csv_bytes.decode("utf-8-sig")
        except UnicodeDecodeError as error:
            raise ValueError("The tracer CSV must use UTF-8 encoding") from error

        reader = csv.DictReader(StringIO(csv_text))
        original_headers = reader.fieldnames or []
        canonical_headers = [
            HEADER_ALIASES.get(header.strip(), header.strip())
            for header in original_headers
        ]
        if len(set(canonical_headers)) != len(canonical_headers):
            raise ValueError("CSV contains duplicate tracer columns")
        missing = sorted(REQUIRED_COLUMNS - set(canonical_headers))
        if missing:
            raise ValueError("CSV is missing required columns: " + ", ".join(missing))

        documents = []
        for line_number, original_row in enumerate(reader, start=2):
            values = self._source_values(
                original_headers,
                canonical_headers,
                original_row,
            )
            documents.append(self._tracer_document(values, line_number))

        # The reference file is only about 4,000 rows, so validate every row
        # before writing. A bad row then cannot leave a partly loaded CSV.
        documents_indexed = 0
        completed_batches = 0
        for offset in range(0, len(documents), self.config.seed_batch_size):
            batch = documents[offset : offset + self.config.seed_batch_size]
            documents_indexed += self.opensearch.bulk_index_tracer_sentences(batch)
            completed_batches += 1
        if documents_indexed:
            self.opensearch.refresh_tracer_sentences()

        return {
            "recordsRead": len(documents),
            "tracerDocumentsIndexed": documents_indexed,
            "completedBatches": completed_batches,
            "batchSize": self.config.seed_batch_size,
            "alias": self.config.tracer_alias,
            "embeddingGeneration": "OpenSearch ingest pipeline",
        }

    @staticmethod
    def _source_values(
        original_headers: list,
        canonical_headers: list,
        original_row: dict,
    ) -> dict:
        """Keep only fields allowed by the strict tracer index mapping."""

        canonical_row = {}
        for original, canonical in zip(
            original_headers,
            canonical_headers,
            strict=True,
        ):
            canonical_row[canonical] = original_row.get(original, "")
        values = {name: canonical_row.get(name, "") for name in REQUIRED_COLUMNS}
        for name in OPTIONAL_COLUMNS:
            value = (canonical_row.get(name) or "").strip()
            if value:
                values[name] = value
        return values

    @staticmethod
    def _tracer_document(values: dict, line_number: int) -> dict:
        """Convert one CSV row into the exact OpenSearch source document."""

        try:
            values["tracerId"] = int(values["tracerId"])
            values["isNonMatcher"] = _as_bool(values["isNonMatcher"])
            values["isFormLanguage"] = _as_bool(values["isFormLanguage"])
            record = TracerSentence.model_validate(values)
        except (AttributeError, TypeError, ValueError) as error:
            raise ValueError(
                f"Invalid tracer CSV row {line_number}: {error}"
            ) from error
        return record.model_dump(mode="json", exclude_none=True)
