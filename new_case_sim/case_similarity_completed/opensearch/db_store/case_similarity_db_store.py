from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from case_similarity_completed.opensearch import constants
from case_similarity_completed.opensearch.contracts import CaseSummary, DocumentContext, TspId
from case_similarity_completed.opensearch.errors import PermanentDataError
from ta_utils.logger.CustomLogger import CustomLogger

# Load the case information connected to the tsp_id from the SQS message.
# Example: tsp_id 10 can return application_id 100, a_number 900001, and analysis_group Asylee.
LOAD_DOCUMENT = text(
    """
    SELECT tsp_id, application_id, a_number, analysis_group
    FROM tsp.text_similarity_input
    WHERE tsp_id = :tsp_id
    """
)

# Rebuild one source application's case summary and update the reverse
# relationship on every affected target application.
#
# Input:
#   {
#       "source_application_id": 100,
#       "analysis_group": "Asylee",
#       "similar_case_threshold": 2,
#       "tah_case_threshold": 3
#   }
#
# Tables used:
#   tsp.titan_matches supplies sentence-level relationships and target global
#   IDs. tsp.text_similarity_input confirms target applications belong to the
#   requested analysis group. tsp.titan_similar_cases supplies existing lists
#   and receives the rebuilt source and reverse target summaries.
#
# Example match counts:
#   Application 100 has 1 matching sentence with application 200, 2 with
#   application 300, and 3 with application 400.
#
# Output used by Python for the source application:
#   {
#       "application_id": 100,
#       "similar_case_count": 2,
#       "tah_case_count": 1
#   }
#
# The function also returns the ID and counts for reverse rows that changed.
# Their full JSONB lists remain stored in titan_similar_cases and are not sent
# back to Python. When application 100 has no matches, its stored summary is
# removed and both returned counts are zero.
REFRESH_CASE_SUMMARIES = text(
    """
    SELECT
        refreshed_application_id AS application_id,
        refreshed_similar_case_count AS similar_case_count,
        refreshed_tah_case_count AS tah_case_count
    FROM tsp.refresh_titan_similar_cases(
        :source_application_id,
        :analysis_group,
        :similar_case_threshold,
        :tah_case_threshold
    )
    """
)


class CaseSimilarityDBStore:
    """Read document ownership and rebuild application-level case summaries."""

    def __init__(self, session_factory: Callable[[], Session], custom_logger: CustomLogger) -> None:
        self.session_factory = session_factory
        self.logger = custom_logger

    def load_case_similarity_document_context(self, tsp_id: TspId) -> DocumentContext:
        """Load the application, receipt number, and analysis group for one document.

        Input: tsp_id=100
        Output: DocumentContext(tsp_id=100, application_id=20, a_number=900001,
        analysis_group="Asylee")

        A missing row stops processing because the service cannot determine
        which application owns the document.
        """

        database_tsp_id = int(tsp_id) if isinstance(tsp_id, str) and tsp_id.strip().isdigit() else tsp_id
        with self.session_factory() as session:
            document_record = session.execute(LOAD_DOCUMENT, {"tsp_id": database_tsp_id}).mappings().one_or_none()

        if document_record is None:
            raise PermanentDataError(f"No text_similarity_input row exists for tsp_id={tsp_id}")
        return DocumentContext(
            tsp_id=document_record["tsp_id"],
            application_id=int(document_record["application_id"]),
            a_number=int(document_record["a_number"]),
            analysis_group=str(document_record["analysis_group"]),
        )

    def refresh_bidirectional_case_summaries(
        self, source_application_id: int, analysis_group: str
    ) -> list[CaseSummary]:
        """Rebuild the source summary and mirror its relationships on connected
        applications without rebuilding their unrelated matches.

        Input: source_application_id=100, analysis_group="Asylee"
        Output: CaseSummary rows for application 100 and each connected
        application whose relationship to 100 changed.

        With thresholds 2 and 3, two matching sentence pairs make a similar
        case and three make a TAH case. A source with no matches is returned
        with zero counts but is not stored in titan_similar_cases.
        """

        with self.session_factory() as session:
            # PostgreSQL counts the relationships, locks rows in order, and
            # updates both directions. It returns only the IDs and counts used
            # by this service.
            refreshed_summary_records = (
                session.execute(
                    REFRESH_CASE_SUMMARIES,
                    {
                        "source_application_id": source_application_id,
                        "analysis_group": analysis_group,
                        "similar_case_threshold": constants.MIN_SIMILAR_CASE_COUNT,
                        "tah_case_threshold": constants.MIN_TAH_CASE_COUNT,
                    },
                )
                .mappings()
                .all()
            )
            session.commit()

        return [
            CaseSummary(
                application_id=int(summary_record["application_id"]),
                analysis_group=analysis_group,
                similar_case_count=int(summary_record["similar_case_count"]),
                tah_case_count=int(summary_record["tah_case_count"]),
            )
            for summary_record in refreshed_summary_records
        ]
