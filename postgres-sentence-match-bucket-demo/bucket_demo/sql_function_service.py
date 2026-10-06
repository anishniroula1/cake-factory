from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB

SQL_FUNCTION_INSERT = text(
    """
    SELECT test_matches.upsert_bidirectional_sentence_key_matches(
        :analysisGroup,
        :sourceSentenceKey,
        :matches
    )
    """
).bindparams(
    bindparam("matches", type_=JSONB)
)


def insertMatchesWithSqlFunction(session, request):
    """Call the PostgreSQL function and commit its bidirectional writes."""

    processedCount = session.execute(
        SQL_FUNCTION_INSERT,
        {
            "analysisGroup": request.analysisGroup,
            "sourceSentenceKey": request.sourceSentenceKey,
            "matches": request.matches,
        },
    ).scalar_one()

    session.commit()

    return {
        "method": "sqlFunction",
        "processedRelationships": processedCount,
    }
