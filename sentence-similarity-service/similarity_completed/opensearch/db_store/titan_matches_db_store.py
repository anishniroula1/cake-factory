from collections.abc import Callable

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.contracts import TitanMatchRelationship, TitanMatchSourceGroup, TitanMatchTarget
from similarity_completed.opensearch.errors import PermanentDataError
from similarity_completed.opensearch.retry import retry_operation, safe_error_summary
from ta_utils.logger.CustomLogger import CustomLogger

UPSERT_BIDIRECTIONAL_TITAN_MATCHES = text(
    """
    SELECT tsp.upsert_bidirectional_titan_matches(
        :source_global_id,
        :source_tsp_id,
        :source_application_id,
        :matches
    )
    """
).bindparams(bindparam("matches", type_=JSONB))


class TitanMatchesDBStore:
    """Save Titan sentence matches one PostgreSQL call at a time."""

    def __init__(self, session_factory: Callable[[], Session], custom_logger: CustomLogger) -> None:
        self.session_factory = session_factory
        self.logger = custom_logger

    def save_bidirectional_match_relationships(
        self, match_relationships: list[TitanMatchRelationship]
    ) -> int:
        """Group and save the target matches returned by OpenSearch.

        Input example:
            [
                {
                    "source_global_id": 100,
                    "source_tsp_id": 10,
                    "source_application_id": 1,
                    "target_global_id": 200,
                    "target_tsp_id": 20,
                    "target_application_id": 2,
                    "distance": 0.10,
                },
                {
                    "source_global_id": 100,
                    "source_tsp_id": 10,
                    "source_application_id": 1,
                    "target_global_id": 300,
                    "target_tsp_id": 30,
                    "target_application_id": 3,
                    "distance": 0.08,
                },
            ]

        Output example:
            2

        The method keeps OpenSearch target results grouped under source global
        ID 100. A source with more than 3000 targets is split across multiple
        SQL-function calls. The function saves both directions in PostgreSQL.
        """

        if not match_relationships:
            return 0

        grouped_matches = self._group_relationships_by_source(match_relationships)

        saved_relationship_count = 0
        for source_global_id, source_group in grouped_matches.items():
            target_items = list(source_group["matches"].items())
            batch_count = (
                len(target_items) + constants.TITAN_MATCH_WRITE_BATCH_SIZE - 1
            ) // constants.TITAN_MATCH_WRITE_BATCH_SIZE

            for batch_number, offset in enumerate(
                range(0, len(target_items), constants.TITAN_MATCH_WRITE_BATCH_SIZE),
                start=1,
            ):
                target_batch = dict(
                    target_items[
                        offset : offset + constants.TITAN_MATCH_WRITE_BATCH_SIZE
                    ]
                )
                saved_count = retry_operation(
                    f"Save Titan relationships for source global_id={source_global_id}, "
                    f"batch {batch_number}/{batch_count}",
                    self._call_bidirectional_match_function,
                    source_global_id,
                    source_group["tsp_id"],
                    source_group["application_id"],
                    target_batch,
                    wait_seconds=constants.RETRY_SECONDS,
                    custom_logger=self.logger,
                )
                saved_relationship_count += saved_count

        return saved_relationship_count

    @staticmethod
    def _group_relationships_by_source(
        match_relationships: list[TitanMatchRelationship],
    ) -> dict[int, TitanMatchSourceGroup]:
        """Put each OpenSearch target under the source sentence that found it.

        Input example:
            [
                {"source_global_id": 100, "target_global_id": 200,
                 "source_tsp_id": 10, "target_tsp_id": 20,
                 "source_application_id": 1, "target_application_id": 2,
                 "distance": 0.10},
                {"source_global_id": 100, "target_global_id": 300,
                 "source_tsp_id": 10, "target_tsp_id": 30,
                 "source_application_id": 1, "target_application_id": 3,
                 "distance": 0.08},
            ]

        Output example:
            {
                100: {
                    "tsp_id": 10,
                    "application_id": 1,
                    "matches": {
                        "200": {
                            "tsp_id": 20,
                            "application_id": 2,
                            "distance": 0.10,
                        },
                        "300": {
                            "tsp_id": 30,
                            "application_id": 3,
                            "distance": 0.08,
                        }
                    },
                }
            }

        OpenSearch already excludes the source application and returns target
        occurrences keyed by global ID. This method only reshapes that result
        for the existing PostgreSQL function. If the same target appears twice,
        the smaller distance is kept.
        """

        grouped_matches: dict[int, TitanMatchSourceGroup] = {}
        for relationship in match_relationships:
            source_global_id = relationship["source_global_id"]
            target_global_id = relationship["target_global_id"]
            if (
                source_global_id == target_global_id
                or relationship["source_application_id"]
                == relationship["target_application_id"]
            ):
                continue

            source_group = grouped_matches.setdefault(
                source_global_id,
                {
                    "tsp_id": relationship["source_tsp_id"],
                    "application_id": relationship["source_application_id"],
                    "matches": {},
                },
            )
            target_key = str(target_global_id)
            saved_target = source_group["matches"].get(target_key)
            if saved_target is not None and saved_target["distance"] <= relationship["distance"]:
                continue
            source_group["matches"][target_key] = {
                "tsp_id": relationship["target_tsp_id"],
                "application_id": relationship["target_application_id"],
                "distance": relationship["distance"],
            }
        return grouped_matches

    def _call_bidirectional_match_function(
        self,
        source_global_id: int,
        source_tsp_id: int,
        source_application_id: int,
        matches: dict[str, TitanMatchTarget],
    ) -> int:
        """Run the bidirectional match function and commit its changes.

        Input example:
            source_global_id=100
            source_tsp_id=10
            source_application_id=1
            matches={
                "200": {
                    "tsp_id": 20,
                    "application_id": 2,
                    "distance": 0.10,
                },
                "300": {
                    "tsp_id": 30,
                    "application_id": 3,
                    "distance": 0.0,
                },
            }

        Output example:
            2

        The SQL function saves the two forward matches and their two reverse
        matches. It returns 2 because it counts each pair once. This method
        commits the transaction and returns that count.

        If PostgreSQL rejects the data, this method raises PermanentDataError
        with a safe error message that does not contain the SQL input.
        """

        try:
            with self.session_factory() as session:
                processed_match_count = session.execute(
                    UPSERT_BIDIRECTIONAL_TITAN_MATCHES,
                    {
                        "source_global_id": source_global_id,
                        "source_tsp_id": source_tsp_id,
                        "source_application_id": source_application_id,
                        "matches": matches,
                    },
                ).scalar_one()
                session.commit()
                return int(processed_match_count)
        except IntegrityError as error:
            # Do not include the SQL statement or the complete relationship
            # list in the error that reaches application logs.
            raise PermanentDataError(
                f"Titan matches were rejected: {safe_error_summary(error)}"
            ) from None
