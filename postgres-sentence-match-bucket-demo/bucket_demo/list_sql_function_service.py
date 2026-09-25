import re

from sqlalchemy import bindparam
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB


APPLICATION_ID_PATTERN = re.compile(r"^\S.*$")

LIST_SQL_FUNCTION_INSERT = text(
    """
    SELECT upsert_bidirectional_sentence_match_lists(
        :analysisGroup,
        :sourceGlobalId,
        :sourceTspId,
        :sourceApplicationId,
        :matches
    )
    """
).bindparams(
    bindparam("matches", type_=JSONB)
)


def validateListSqlFunctionRequest(request):
    """Validate the complete request for the JSONB-list SQL function."""

    if not request.analysisGroup.strip():
        raise ValueError("analysisGroup is required")

    if isinstance(request.sourceGlobalId, bool) or request.sourceGlobalId < 1:
        raise ValueError("sourceGlobalId must be a positive number")

    if isinstance(request.sourceTspId, bool) or request.sourceTspId < 1:
        raise ValueError("sourceTspId must be a positive number")

    if not APPLICATION_ID_PATTERN.fullmatch(request.sourceApplicationId):
        raise ValueError("sourceApplicationId is required")

    if not request.matches:
        raise ValueError("matches must contain at least one match object")

    receivedGlobalIds = set()

    for match in request.matches:
        if not isinstance(match, dict):
            raise ValueError("every match must be an object")

        requiredKeys = {
            "global_id",
            "distance",
            "tsp_id",
            "application_id",
        }

        if not requiredKeys.issubset(match):
            raise ValueError(
                "every match needs global_id, distance, tsp_id, "
                "and application_id"
            )

        globalId = match["global_id"]
        tspId = match["tsp_id"]
        distance = match["distance"]
        applicationId = match["application_id"]

        if isinstance(globalId, bool) or not isinstance(globalId, int):
            raise ValueError("every global_id must be a number")

        if globalId < 1 or globalId == request.sourceGlobalId:
            raise ValueError("global_id must be positive and cannot match itself")

        if globalId in receivedGlobalIds:
            raise ValueError("matches cannot contain the same global_id twice")

        receivedGlobalIds.add(globalId)

        if isinstance(tspId, bool) or not isinstance(tspId, int) or tspId < 1:
            raise ValueError("every tsp_id must be a positive number")

        if isinstance(distance, bool) or not isinstance(distance, (int, float)):
            raise ValueError("every distance must be a number")

        if distance < 0 or distance > 2:
            raise ValueError("every distance must be between 0 and 2")

        if not isinstance(applicationId, str) or not applicationId.strip():
            raise ValueError("every application_id is required")


def readListSqlFunctionSource(session, request):
    """Read the source row after the SQL function commits."""

    row = session.execute(
        text(
            """
            SELECT tsp_id, match_count, matches
            FROM matches
            WHERE analysis_group = :analysisGroup
              AND global_id = :sourceGlobalId
            """
        ),
        {
            "analysisGroup": request.analysisGroup,
            "sourceGlobalId": request.sourceGlobalId,
        },
    ).mappings().one()

    return {
        "globalId": request.sourceGlobalId,
        "tspId": int(row["tsp_id"]),
        "matchCount": int(row["match_count"]),
        "matches": row["matches"],
    }


def readListSqlFunctionReverse(session, request):
    """Read every target row's saved source object in one query."""

    statement = text(
        """
        SELECT
            saved.global_id,
            saved.tsp_id,
            match_item
        FROM matches AS saved
        CROSS JOIN LATERAL jsonb_array_elements(saved.matches)
            AS item(match_item)
        WHERE saved.analysis_group = :analysisGroup
          AND saved.global_id IN :targetGlobalIds
          AND (match_item ->> 'global_id')::BIGINT = :sourceGlobalId
        ORDER BY saved.global_id
        """
    ).bindparams(
        bindparam("targetGlobalIds", expanding=True)
    )

    rows = session.execute(
        statement,
        {
            "analysisGroup": request.analysisGroup,
            "sourceGlobalId": request.sourceGlobalId,
            "targetGlobalIds": [
                match["global_id"]
                for match in request.matches
            ],
        },
    ).mappings().all()

    reverseMatches = []

    for row in rows:
        reverseMatches.append(
            {
                "analysisGroup": request.analysisGroup,
                "globalId": int(row["global_id"]),
                "tspId": int(row["tsp_id"]),
                "savedMatch": row["match_item"],
            }
        )

    return reverseMatches


def insertListMatchesWithSqlFunction(session, request):
    """Run the complete production-style JSONB-list SQL flow."""

    validateListSqlFunctionRequest(request)

    processedCount = session.execute(
        LIST_SQL_FUNCTION_INSERT,
        {
            "analysisGroup": request.analysisGroup,
            "sourceGlobalId": request.sourceGlobalId,
            "sourceTspId": request.sourceTspId,
            "sourceApplicationId": request.sourceApplicationId,
            "matches": request.matches,
        },
    ).scalar_one()

    session.commit()

    return {
        "method": "sqlFunctionJsonbList",
        "processedRelationships": processedCount,
        "source": readListSqlFunctionSource(session, request),
        "reverseMatches": readListSqlFunctionReverse(session, request),
    }
