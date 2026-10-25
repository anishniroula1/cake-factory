from collections.abc import Callable

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from transform_completed.opensearch.contracts import ApplicationReference, TspId

# Find the application and A-number connected to the tsp_id from the SQS
# message.
#
# Input: {"tsp_id": 10}
# Table used: tsp.text_similarity_input
# Output: {"application_id": 50, "a_number": 900001,
#          "analysis_group": "Asylee"}
#
# Only the current document row is used. No row means API Transform cannot
# determine which application should be rebuilt.
LOAD_APPLICATION_REFERENCE = text(
    """
    SELECT application_id, a_number, analysis_group
    FROM tsp.text_similarity_input
    WHERE tsp_id = :tsp_id
      AND is_current = TRUE
    """
)

# Build the application-level section placed in the API Kafka payload.
#
# Input:
#   {"application_ids": [50], "a_numbers": [900001]}
#
# Tables used:
#   landing.application_id_lookup supplies the application's A-number, analysis
#   group, and case type. landing.receipt_number_lookup supplies its receipt.
#   biodata.applicant supplies case details. tsp.text_similarity_input supplies
#   the sentence source types. tsp.titan_similar_cases supplies Titan case
#   counts, application IDs, and TAH status. landing.global_case_details_history
#   supplies confidence values.
#
# Output example:
#   {
#       "application_id": 50,
#       "a_number": 900001,
#       "receipt_number": "A100000001",
#       "similar_cases_count": 2,
#       "similar_case_ids": [900010, 900020],
#       "similar_case_application_ids": [60, 70],
#       "ta_hit": true
#   }
#
# titan_similar_cases stores application IDs. The lateral query maps the
# 2-or-more-match IDs to A-numbers because the existing API contract calls that
# field similar_case_ids but expects A-numbers in it.
LOAD_APPLICATION_RECORDS = text(
    """
    WITH confidence AS (
        SELECT
            applicant_key,
            MIN(CASE WHEN confidence_score = 1 THEN 0 ELSE 1 END) AS global_confidence_score,
            MIN(confidence_score) AS confidence_score
        FROM landing.global_case_details_history
        WHERE a_number IN :a_numbers
        GROUP BY applicant_key
    ),
    sentence_sources AS (
        SELECT
            application_id,
            SUBSTRING(STRING_AGG(DISTINCT source_type, ' and '), 1, 50) AS sentences_source_type
        FROM tsp.text_similarity_input
        WHERE application_id IN :application_ids
          AND is_current = TRUE
        GROUP BY application_id
    )
    SELECT
        lookup.application_id,
        lookup.a_number,
        receipt.receipt_number,
        lookup.analysis_group,
        COALESCE(summary.similar_case_count, 0) > 0 AS similarity_found,
        applicant.preparer_name,
        applicant.preparer_address,
        applicant.attorney,
        applicant.case_control_office,
        applicant.filing_date,
        applicant.case_state,
        COALESCE(summary.similar_case_count, 0) AS similar_cases_count,
        COALESCE(confidence.global_confidence_score, 0) AS global_confidence_score,
        COALESCE(mapped_cases.a_numbers, '[]'::JSONB) AS similar_case_ids,
        COALESCE(summary.similar_application_ids, '[]'::JSONB) AS similar_case_application_ids,
        confidence.confidence_score,
        applicant.source_type AS applicant_details_source_type,
        sentence_sources.sentences_source_type,
        UPPER(
            COALESCE(
                CASE WHEN lookup.analysis_group = 'Refugee' THEN 'REFUGEE' ELSE lookup.case_type END,
                'AFFIRMATIVE'
            )
        ) AS case_type,
        COALESCE(summary.is_tah, FALSE) AS ta_hit
    FROM landing.application_id_lookup AS lookup
    JOIN sentence_sources ON sentence_sources.application_id = lookup.application_id
    LEFT JOIN landing.receipt_number_lookup AS receipt ON receipt.application_id = lookup.application_id
    LEFT JOIN biodata.applicant AS applicant
        ON applicant.application_id = lookup.application_id
       AND applicant.is_current = TRUE
    LEFT JOIN tsp.titan_similar_cases AS summary ON summary.application_id = lookup.application_id
    LEFT JOIN confidence ON confidence.applicant_key = applicant.applicant_key
    LEFT JOIN LATERAL (
        SELECT COALESCE(JSONB_AGG(target_lookup.a_number ORDER BY target_lookup.a_number), '[]'::JSONB) AS a_numbers
        FROM JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(summary.similar_case_ids, '[]'::JSONB)) AS target(application_id)
        JOIN landing.application_id_lookup AS target_lookup
          ON target_lookup.application_id = target.application_id::INTEGER
         AND target_lookup.analysis_group = lookup.analysis_group
    ) AS mapped_cases ON TRUE
    WHERE lookup.application_id IN :application_ids
    ORDER BY lookup.application_id
    """
).bindparams(bindparam("application_ids", expanding=True), bindparam("a_numbers", expanding=True))


class ApplicationDBStore:
    """Resolve document ownership and load application-level API records."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def load_application_reference(self, tsp_id: TspId) -> ApplicationReference | None:
        """Find the application affected by one incoming document ID.

        Input: tsp_id=10
        Output: ApplicationReference(application_id=50, a_number=900001,
        analysis_group="Asylee"), or None when the document does not exist.
        """

        with self.session_factory() as session:
            application_reference_record = (
                session.execute(LOAD_APPLICATION_REFERENCE, {"tsp_id": tsp_id}).mappings().one_or_none()
            )
        if application_reference_record is None:
            return None
        return ApplicationReference(
            application_id=int(application_reference_record["application_id"]),
            a_number=int(application_reference_record["a_number"]),
            analysis_group=str(application_reference_record["analysis_group"]),
        )

    def load_application_payload_records(self, application_references: list[ApplicationReference]) -> dict[int, dict]:
        """Load the case fields placed in each Kafka event's a_records section.

        Input: references=[ApplicationReference(application_id=50,
        a_number=900001, analysis_group="Asylee")]
        Output: {50: {"a_number": 900001, "similar_case_ids": [...], ...}}

        Titan case lists contain application IDs. The query also maps those IDs
        to the A-number list required by the existing API contract.
        """

        if not application_references:
            return {}
        query_parameters = {
            "application_ids": [reference.application_id for reference in application_references],
            "a_numbers": [reference.a_number for reference in application_references],
        }
        with self.session_factory() as session:
            application_payload_records = session.execute(LOAD_APPLICATION_RECORDS, query_parameters).mappings().all()
        return {
            int(application_payload_record["application_id"]): dict(application_payload_record)
            for application_payload_record in application_payload_records
        }
