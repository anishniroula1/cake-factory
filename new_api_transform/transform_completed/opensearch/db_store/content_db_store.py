import math
from collections.abc import Callable
from datetime import datetime

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from transform_completed.opensearch import constants

# Load current sentences and rebuild each sentence's existing API match list
# from all of its Titan JSONB buckets.
#
# Input:
#   {"application_ids": [50], "sentence_match_limit": 3000}
#
# Tables used:
#   tsp.sentences_vector supplies sentence text and IDs.
#   tsp.text_similarity_input supplies application, document, section, and
#   template values. tsp.api_section_map maps internal sections to API sections.
#   tsp.titan_matches supplies target global IDs and cosine distances.
#
# Example Titan buckets for source global_id 10:
#   bucket 0 has {"20": 0.0}; bucket 1 has {"30": 0.12}
#
# Output example:
#   {
#       "global_id": 10,
#       "application_id": 50,
#       "tsp_id": 100,
#       "sentence": "The applicant entered the country in 2020.",
#       "matches": [
#           {"global_id": 20, "distance": 0.0},
#           {"global_id": 30, "distance": 0.12}
#       ]
#   }
#
# The per-sentence limit keeps one unusually common sentence from making the
# Kafka event too large.
LOAD_SENTENCES = text(
    """
    WITH source_sentences AS (
        SELECT
            sentence.global_id,
            sentence.sent_id_local,
            input.application_id,
            sentence.tsp_id,
            COALESCE(section.api_section_name, input.section_name) AS section,
            sentence.sentence_content AS sentence,
            sentence.last_modified AS sentences_last_modified,
            input.is_template
        FROM tsp.sentences_vector AS sentence
        JOIN tsp.text_similarity_input AS input
          ON input.tsp_id = sentence.tsp_id
         AND input.is_current = TRUE
        LEFT JOIN tsp.api_section_map AS section
          ON section.tsp_section_name = input.section_name
        WHERE input.application_id IN :application_ids
          AND sentence.is_current = TRUE
    )
    SELECT
        source.global_id,
        source.sent_id_local AS local_id,
        source.application_id,
        source.tsp_id,
        source.section,
        source.sentence,
        source.sentences_last_modified,
        COALESCE(matches.matches_count, 0) AS matches_count,
        COALESCE(matches.matches, '[]'::JSONB) AS matches,
        matches.matches_last_modified,
        source.is_template
    FROM source_sentences AS source
    LEFT JOIN LATERAL (
        SELECT
            COUNT(*) AS matches_count,
            JSONB_AGG(
                JSONB_BUILD_OBJECT(
                    'global_id', limited.target_global_id,
                    'distance', limited.distance
                )
                ORDER BY limited.target_global_id
            ) AS matches,
            MAX(limited.last_modified) AS matches_last_modified
        FROM (
            SELECT
                match_item.key::INTEGER AS target_global_id,
                (match_item.value #>> '{}')::DOUBLE PRECISION AS distance,
                bucket.last_modified
            FROM tsp.titan_matches AS bucket
            CROSS JOIN LATERAL JSONB_EACH(bucket.matches) AS match_item(key, value)
            WHERE bucket.global_id = source.global_id
            ORDER BY bucket.match_bucket_number, match_item.key::INTEGER
            LIMIT :sentence_match_limit
        ) AS limited
    ) AS matches ON TRUE
    ORDER BY source.application_id, source.tsp_id, source.sent_id_local
    """
).bindparams(bindparam("application_ids", expanding=True))

# Load every current document for the requested applications and count how
# many distinct source sentences have at least one Titan match.
#
# Input: {"application_ids": [50]}
#
# Tables used:
#   tsp.text_similarity_input supplies document values.
#   tsp.titan_matches supplies matched source global IDs.
#   landing.global_document_registry supplies file names.
#   tsp.api_section_map maps internal section names to API section names.
#
# Output example:
#   {
#       "application_id": 50,
#       "tsp_id": 100,
#       "section_name": "Affidavit",
#       "file_name": "affidavit.pdf",
#       "match_count": 3
#   }
LOAD_DOCUMENTS = text(
    f"""
    SELECT
        input.application_id,
        COALESCE(section.api_section_name, input.section_name) AS section_name,
        input.tsp_id,
        TO_CHAR(input.load_date, '{constants.DATETIME_FORMAT_TZH}') AS load_date,
        COALESCE(registry.file_name, 'unknown') AS file_name,
        COUNT(DISTINCT bucket.global_id) FILTER (WHERE bucket.match_count > 0) AS match_count
    FROM tsp.text_similarity_input AS input
    LEFT JOIN tsp.titan_matches AS bucket ON bucket.tsp_id = input.tsp_id
    LEFT JOIN landing.global_document_registry AS registry
      ON registry.global_document_registry_id = input.source_id
     AND input.source_type <> 'IVCS'
    LEFT JOIN tsp.api_section_map AS section ON section.tsp_section_name = input.section_name
    WHERE input.application_id IN :application_ids
      AND input.is_current = TRUE
    GROUP BY
        input.application_id,
        input.tsp_id,
        input.section_name,
        section.api_section_name,
        registry.file_name
    ORDER BY input.application_id, input.tsp_id
    """
).bindparams(bindparam("application_ids", expanding=True))

# Load public-template metadata for Asylee applications.
#
# Input: {"application_ids": [50]}
# Tables used: tsp.text_similarity_input and landing.template_registry
# Output: {"application_id": 50, "a_number": 900001,
#          "url": "https://example/template.pdf", "load_date": "..."}
#
# Rows are newest first. The Python store keeps the first row for each
# application, which gives the API payload its newest public template.
LOAD_PUBLIC_TEMPLATES = text(
    f"""
    SELECT
        input.application_id,
        input.a_number,
        template.template_url AS url,
        TO_CHAR(input.load_date, '{constants.DATETIME_FORMAT_TZH}') AS load_date
    FROM tsp.text_similarity_input AS input
    JOIN landing.template_registry AS template ON template.a_number = input.a_number
    WHERE input.application_id IN :application_ids
      AND input.is_current = TRUE
    ORDER BY input.application_id, input.load_date DESC
    """
).bindparams(bindparam("application_ids", expanding=True))


class ContentDBStore:
    """Load documents, templates, sentences, and Titan-backed sentence matches."""

    def __init__(self, session_factory: Callable[[], Session], sentence_match_limit: int) -> None:
        self.session_factory = session_factory
        self.sentence_match_limit = sentence_match_limit

    def load_sentences(self, application_ids: list[int], reduce_kafka_size: bool) -> dict[int, list[dict]]:
        """Build the existing sentence response from all 16 Titan match buckets.

        Input: application_ids=[50], reduce_kafka_size=False, with bucket values
        {"20": 0.0} and {"30": 0.12}
        Output: {50: [{..., "matches": [{"global_id": 20, "distance": 0.0},
        {"global_id": 30, "distance": 0.12}]}]}
        """

        if not application_ids:
            return {}
        with self.session_factory() as session:
            rows = (
                session.execute(
                    LOAD_SENTENCES,
                    {"application_ids": application_ids, "sentence_match_limit": self.sentence_match_limit},
                )
                .mappings()
                .all()
            )
        grouped: dict[int, list[dict]] = {}
        for row in rows:
            sentence = dict(row)
            application_id = int(sentence.pop("application_id"))
            matches_modified = sentence.pop("matches_last_modified", None)
            sentence_modified = sentence.pop("sentences_last_modified", None)
            sentence["matches"] = [self._small_match(match) for match in sentence.get("matches") or []]
            if reduce_kafka_size and self._is_match_only_update(matches_modified, sentence_modified):
                sentence = {"global_id": sentence["global_id"], "matches": sentence["matches"], "op": "u"}
            grouped.setdefault(application_id, []).append(sentence)
        return grouped

    def load_documents(self, application_ids: list[int]) -> dict[int, list[dict]]:
        """Load document metadata and count sentences with at least one Titan match.

        Input: application_ids=[50]
        Output: {50: [{"tsp_id": 10, "section_name": "Affidavit",
        "match_count": 3, ...}]}
        """

        return self._load_grouped(LOAD_DOCUMENTS, application_ids)

    def load_public_templates(self, application_ids: list[int]) -> dict[int, dict]:
        """Load the newest public-template row for each Asylee application.

        Input: application_ids=[50]
        Output: {50: {"a_number": 900001, "url": "https://...", ...}}
        """

        if not application_ids:
            return {}
        with self.session_factory() as session:
            rows = session.execute(LOAD_PUBLIC_TEMPLATES, {"application_ids": application_ids}).mappings().all()
        templates: dict[int, dict] = {}
        for row in rows:
            values = dict(row)
            application_id = int(values.pop("application_id"))
            templates.setdefault(application_id, values)
        return templates

    def _load_grouped(self, statement, application_ids: list[int]) -> dict[int, list[dict]]:
        """Execute a shared query and group its rows by application ID.

        Input: application_ids=[50, 60]
        Output: {50: [rows for 50], 60: [rows for 60]}
        """

        if not application_ids:
            return {}
        with self.session_factory() as session:
            rows = session.execute(statement, {"application_ids": application_ids}).mappings().all()
        grouped: dict[int, list[dict]] = {}
        for row in rows:
            values = dict(row)
            application_id = int(values.pop("application_id"))
            grouped.setdefault(application_id, []).append(values)
        return grouped

    @staticmethod
    def _small_match(match: dict) -> dict:
        """Keep API match fields and floor distance to four decimal places.

        Input: match={"global_id": "20", "distance": 0.123456}
        Output: {"global_id": 20, "distance": 0.1234}
        """

        distance = float(match["distance"])
        return {"global_id": int(match["global_id"]), "distance": math.floor(distance * 10000) / 10000}

    @staticmethod
    def _is_match_only_update(matches_modified: datetime | None, sentence_modified: datetime | None) -> bool:
        """Determine whether only sentence-match data changed.

        Input: matches_modified more than 30 minutes after sentence_modified.
        Output: True
        """

        if matches_modified is None or sentence_modified is None:
            return False
        return (matches_modified - sentence_modified).total_seconds() > 30 * 60
