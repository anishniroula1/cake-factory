import json

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import bindparam
from sqlalchemy import cast
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import union_all
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.dialects.postgresql import insert


listPythonMetadata = MetaData()

listPythonMatches = Table(
    "titan_matches",
    listPythonMetadata,
    Column("global_id", Integer, primary_key=True),
    Column("tsp_id", Integer, nullable=False),
    Column("matches", JSONB, nullable=False),
    Column("match_count", Integer),
    Column("verbatim", ARRAY(Integer)),
    Column("last_modified", DateTime(timezone=True)),
)


def validateListPythonRequest(request):
    """Validate owner IDs and the four fields saved for every match."""

    if isinstance(request.sourceGlobalId, bool) or request.sourceGlobalId < 1:
        raise ValueError("sourceGlobalId must be a positive number")

    if isinstance(request.sourceTspId, bool) or request.sourceTspId < 1:
        raise ValueError("sourceTspId must be a positive number")

    if isinstance(request.sourceANumber, bool) or request.sourceANumber < 1:
        raise ValueError("sourceANumber must be a positive number")

    receivedGlobalIds = set()

    for match in request.matches:
        if not isinstance(match, dict):
            raise ValueError("every match must be an object")

        requiredKeys = {
            "global_id",
            "distance",
            "tsp_id",
            "a_number",
        }

        if set(match) != requiredKeys:
            raise ValueError(
                "every match must contain only global_id, distance, "
                "tsp_id, and a_number"
            )

        globalId = match["global_id"]
        tspId = match["tsp_id"]
        distance = match["distance"]
        aNumber = match["a_number"]

        if isinstance(globalId, bool) or not isinstance(globalId, int):
            raise ValueError("every global_id must be a number")

        if globalId < 1 or globalId == request.sourceGlobalId:
            raise ValueError("global_id must be positive and cannot match itself")

        if globalId in receivedGlobalIds:
            raise ValueError("matches cannot contain the same global_id twice")

        receivedGlobalIds.add(globalId)

        if isinstance(tspId, bool) or not isinstance(tspId, int) or tspId < 1:
            raise ValueError("every tsp_id must be a positive number")

        if isinstance(aNumber, bool) or not isinstance(aNumber, int):
            raise ValueError("every a_number must be a number")

        if aNumber < 1:
            raise ValueError("every a_number must be a positive number")

        if isinstance(distance, bool) or not isinstance(distance, (int, float)):
            raise ValueError("every distance must be a number")

        if distance < 0 or distance > 2:
            raise ValueError("every distance must be between 0 and 2")


def buildListPythonOwnerRows(request):
    """Build one insert row for the source and every reverse owner."""

    ownerRows = {
        request.sourceGlobalId: {
            "globalId": request.sourceGlobalId,
            "tspId": request.sourceTspId,
            "incomingMatches": [],
        }
    }

    for match in request.matches:
        ownerRows[request.sourceGlobalId]["incomingMatches"].append(match)
        ownerRows[match["global_id"]] = {
            "globalId": match["global_id"],
            "tspId": match["tsp_id"],
            "incomingMatches": [
                {
                    "global_id": request.sourceGlobalId,
                    "distance": match["distance"],
                    "tsp_id": request.sourceTspId,
                    "a_number": request.sourceANumber,
                }
            ],
        }

    return [ownerRows[globalId] for globalId in sorted(ownerRows)]


def buildListPythonUpsertStatement():
    """Create missing owners and merge only matches for existing owners."""

    baseInsert = insert(listPythonMatches).values(
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
        Integer,
    )
    incomingGlobalId = cast(
        cast(incomingItems.c.match_item, JSONB)["global_id"].astext,
        Integer,
    )

    incomingGlobalIdExists = (
        select(incomingItems.c.match_item)
        .select_from(incomingItems)
        .where(incomingGlobalId == savedGlobalId)
        .exists()
    )

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
        index_elements=[listPythonMatches.c.global_id],
        set_={
            "matches": mergedMatches,
            "last_modified": func.now(),
        },
    )


def readListPythonSource(session, request):
    """Read the source row and its PostgreSQL-generated values."""

    row = session.execute(
        text(
            """
            SELECT
                tsp_id,
                match_count,
                matches,
                verbatim,
                last_modified
            FROM titan_matches
            WHERE global_id = :sourceGlobalId
            """
        ),
        {"sourceGlobalId": request.sourceGlobalId},
    ).mappings().one()

    return {
        "globalId": request.sourceGlobalId,
        "tspId": int(row["tsp_id"]),
        "matchCount": int(row["match_count"]),
        "matches": row["matches"],
        "verbatim": row["verbatim"],
        "lastModified": row["last_modified"],
    }


def readListPythonReverse(session, request):
    """Read the source relationship saved in every target row."""

    if not request.matches:
        return []

    statement = text(
        """
        SELECT
            saved.global_id,
            saved.tsp_id,
            saved.match_count,
            saved.verbatim,
            saved.last_modified,
            match_item
        FROM titan_matches AS saved
        CROSS JOIN LATERAL jsonb_array_elements(saved.matches)
            AS item(match_item)
        WHERE saved.global_id IN :targetGlobalIds
          AND (match_item ->> 'global_id')::INTEGER = :sourceGlobalId
        ORDER BY saved.global_id
        """
    ).bindparams(
        bindparam("targetGlobalIds", expanding=True)
    )

    rows = session.execute(
        statement,
        {
            "sourceGlobalId": request.sourceGlobalId,
            "targetGlobalIds": [
                match["global_id"]
                for match in request.matches
            ],
        },
    ).mappings().all()

    return [
        {
            "globalId": int(row["global_id"]),
            "tspId": int(row["tsp_id"]),
            "matchCount": int(row["match_count"]),
            "verbatim": row["verbatim"],
            "lastModified": row["last_modified"],
            "savedMatch": row["match_item"],
        }
        for row in rows
    ]


def insertListMatchesWithPython(session, request):
    """Save source and reverse rows through one SQLAlchemy bulk upsert."""

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
