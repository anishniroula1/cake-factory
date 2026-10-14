from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from case_similarity_service.opensearch.config import Config
from case_similarity_service.opensearch.contracts import CaseSummary, DocumentContext, TspId
from case_similarity_service.opensearch.errors import PermanentDataError
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

# This database function fully rebuilds the source application's summary.
# Connected applications keep their existing summaries; the function only adds
# or removes the source application from their three saved lists.
#
# Example: application 100 matches applications 200 and 300. The function
# rebuilds 100, then mirrors only the relationship to 100 on rows 200 and 300.
# If 100 has no match, no summary row is saved and the function returns empty
# lists so the service can still complete the document.
REFRESH_CASE_SUMMARIES = text(
    """
    SELECT
        refreshed_application_id AS application_id,
        refreshed_similar_application_ids AS similar_application_ids,
        refreshed_similar_case_ids AS similar_case_ids,
        refreshed_similar_case_count AS similar_case_count,
        refreshed_tah_case_ids AS tah_case_ids,
        refreshed_tah_case_count AS tah_case_count,
        refreshed_is_tah AS is_tah
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

    def __init__(self, config: Config, session_factory: Callable[[], Session], custom_logger: CustomLogger) -> None:
        self.config = config
        self.session_factory = session_factory
        self.logger = custom_logger

    def load_document_context(self, tsp_id: TspId) -> DocumentContext:
        """Load the application, receipt number, and analysis group for one document.

        Input: tsp_id=100
        Output: DocumentContext(tsp_id=100, application_id=20, a_number=900001,
        analysis_group="Asylee")

        A missing row stops processing because the service cannot determine
        which application owns the document.
        """

        with self.session_factory() as session:
            # one_or_none() makes an unexpected duplicate fail instead of silently
            # selecting the wrong application.
            row = session.execute(LOAD_DOCUMENT, {"tsp_id": tsp_id}).mappings().one_or_none()
        if row is None:
            raise PermanentDataError(f"No text_similarity_input row exists for tsp_id={tsp_id}")

        # Convert database values into the small object used by the handler.
        return DocumentContext(
            tsp_id=row["tsp_id"],
            application_id=int(row["application_id"]),
            a_number=int(row["a_number"]),
            analysis_group=str(row["analysis_group"]),
        )

    def refresh_case_summaries(self, source_application_id: int, analysis_group: str) -> list[CaseSummary]:
        """Rebuild the source summary and mirror its relationships on connected
        applications without rebuilding their unrelated matches.

        Input: source_application_id=100, analysis_group="Asylee"
        Output: CaseSummary rows for application 100 and each connected
        application whose relationship to 100 changed.

        With thresholds 2 and 3, one matching sentence adds the target only to
        similar_application_ids, two also add it to similar_case_ids, and three
        also add it to tah_case_ids. A source with no matches is returned with
        empty lists but is not stored in titan_similar_cases.
        """

        with self.session_factory() as session:
            # PostgreSQL counts the source relationships, locks rows in order,
            # and updates both directions. It returns the final rows after the
            # generated columns calculate their counts and TAH values.
            rows = (
                session.execute(
                    REFRESH_CASE_SUMMARIES,
                    {
                        "source_application_id": source_application_id,
                        "analysis_group": analysis_group,
                        "similar_case_threshold": self.config.min_similar_case_count,
                        "tah_case_threshold": self.config.min_tah_case_count,
                    },
                )
                .mappings()
                .all()
            )
            session.commit()

        # JSONB arrays come back as normal Python lists. Convert every ID to int
        # so callers always receive the same type.
        return [
            CaseSummary(
                application_id=int(row["application_id"]),
                analysis_group=analysis_group,
                similar_application_ids=[int(value) for value in row["similar_application_ids"]],
                similar_case_ids=[int(value) for value in row["similar_case_ids"]],
                tah_case_ids=[int(value) for value in row["tah_case_ids"]],
            )
            for row in rows
        ]
