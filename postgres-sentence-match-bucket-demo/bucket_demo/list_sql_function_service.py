from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB

LIST_SQL_FUNCTION_INSERT = text(
    """
    SELECT test_matches.upsert_bidirectional_titan_matches(
        :sourceGlobalId,
        :sourceTspId,
        :sourceANumber,
        :matches
    )
    """
).bindparams(
    bindparam("matches", type_=JSONB)
)


def insertListMatchesWithSqlFunction(session, request):
    """Call the JSONB-list function and commit its bidirectional writes."""

    processedCount = session.execute(
        LIST_SQL_FUNCTION_INSERT,
        {
            "sourceGlobalId": request.sourceGlobalId,
            "sourceTspId": request.sourceTspId,
            "sourceANumber": request.sourceANumber,
            "matches": request.matches,
        },
    ).scalar_one()

    session.commit()

    return {
        "method": "sqlFunctionJsonbList",
        "processedRelationships": processedCount,
    }
