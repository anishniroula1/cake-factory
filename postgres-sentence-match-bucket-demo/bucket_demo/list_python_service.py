import json
import re

from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import bindparam
from sqlalchemy import cast
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import union_all
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.dialects.postgresql import insert


APPLICATION_ID_PATTERN = re.compile(r"^\S.*$")

listPythonMetadata = MetaData()

listPythonMatches = Table(
    "matches",
    listPythonMetadata,
    Column("analysis_group", Text, primary_key=True),
    Column("global_id", BigInteger, primary_key=True),
    Column("tsp_id", BigInteger, nullable=False),
    Column("matches", JSONB, nullable=False),
    Column("match_count", Integer),
    Column("created_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
)


def validateListPythonRequest(request):
    """Validate the complete request for the JSONB-list Python flow."""

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


def buildListPythonOwnerRows(request):
    """Group all forward and reverse matches into one row per owner."""

    ownerRows = {
        (request.analysisGroup, request.sourceGlobalId): {
            "analysisGroup": request.analysisGroup,
            "globalId": request.sourceGlobalId,
            "tspId": request.sourceTspId,
            "incomingMatches": [],
        }
    }

    for match in request.matches:
        ownerRows[
            (request.analysisGroup, request.sourceGlobalId)
        ]["incomingMatches"].append(match)

        identity = (
            request.analysisGroup,
            match["global_id"],
        )
        ownerRows[identity] = {
            "analysisGroup": request.analysisGroup,
            "globalId": match["global_id"],
            "tspId": match["tsp_id"],
            "incomingMatches": [
                {
                    "global_id": request.sourceGlobalId,
                    "distance": match["distance"],
                    "tsp_id": request.sourceTspId,
                    "application_id": request.sourceApplicationId,
                }
            ],
        }
    # print([ownerRows[identity] for identity in sorted(ownerRows)])
    return [ownerRows[identity] for identity in sorted(ownerRows)]


def buildListPythonUpsertStatement():
    """Build one upsert that creates owners and merges their match lists."""

    baseInsert = insert(listPythonMatches).values(
        analysis_group=bindparam("analysisGroup"),
        global_id=bindparam("globalId"),
        tsp_id=bindparam("tspId"),
        matches=bindparam("incomingMatches", type_=JSONB),
    )

    savedItems = (
        func.jsonb_array_elements(listPythonMatches.c.matches)
        .table_valued("match_item")
        .render_derived(name="saved_items")
    )
    incomingItems = (
        func.jsonb_array_elements(baseInsert.excluded.matches)
        .table_valued("match_item")
        .render_derived(name="incoming_items")
    )

    savedGlobalId = cast(
        cast(savedItems.c.match_item, JSONB)["global_id"].astext,
        BigInteger,
    )
    incomingGlobalId = cast(
        cast(incomingItems.c.match_item, JSONB)["global_id"].astext,
        BigInteger,
    )

    incomingGlobalIdExists = (
        select(incomingItems.c.match_item)
        .select_from(incomingItems)
        .where(incomingGlobalId == savedGlobalId)
        .exists()
    )

    # Keep saved objects that are not included in this request. Then add all
    # incoming objects. This makes the latest distance win for the same ID.
    existingItemsToKeep = (
        select(
            savedGlobalId.label("match_global_id"),
            savedItems.c.match_item.label("match_item"),
        )
        .select_from(savedItems)
        .where(~incomingGlobalIdExists)
    )
    newAndUpdatedItems = select(
        incomingGlobalId.label("match_global_id"),
        incomingItems.c.match_item.label("match_item"),
    ).select_from(incomingItems)

    combinedItems = union_all(
        existingItemsToKeep,
        newAndUpdatedItems,
    ).subquery("combined_items")

    mergedMatches = (
        select(
            func.jsonb_agg(
                aggregate_order_by(
                    combinedItems.c.match_item,
                    combinedItems.c.match_global_id,
                )
            )
        )
        .select_from(combinedItems)
        .scalar_subquery()
    )

    return baseInsert.on_conflict_do_update(
        index_elements=[
            listPythonMatches.c.analysis_group,
            listPythonMatches.c.global_id,
        ],
        set_={
            "tsp_id": baseInsert.excluded.tsp_id,
            "matches": mergedMatches,
            "updated_at": func.now(),
        },
    )


def readListPythonSource(session, request):
    """Read the source row after the Python transaction commits."""

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


def readListPythonReverse(session, request):
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


def insertListMatchesWithPython(session, request):
    """Run the complete production-style JSONB-list Python flow."""

    validateListPythonRequest(request)
    ownerRows = buildListPythonOwnerRows(request)
    print(json.dumps(ownerRows, indent=4))

    session.execute(buildListPythonUpsertStatement(), ownerRows)
    session.commit()

    return {
        "method": "pythonBindParametersJsonbList",
        "processedRelationships": len(request.matches),
        "source": readListPythonSource(session, request),
        "reverseMatches": readListPythonReverse(session, request),
    }
