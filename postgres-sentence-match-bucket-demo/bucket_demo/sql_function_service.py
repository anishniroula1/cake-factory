import re

from sqlalchemy import bindparam
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB


SHA_256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

SQL_FUNCTION_INSERT = text(
    """
    SELECT upsert_bidirectional_sentence_key_matches(
        :analysisGroup,
        :sourceSentenceKey,
        :matches
    )
    """
).bindparams(
    bindparam("matches", type_=JSONB)
)


def validateSqlFunctionRequest(request):
    """Validate values used by the PostgreSQL-function endpoint."""

    if not request.analysisGroup.strip():
        raise ValueError("analysisGroup is required")

    if not SHA_256_PATTERN.fullmatch(request.sourceSentenceKey):
        raise ValueError(
            "sourceSentenceKey must be a lowercase 64-character SHA-256 key"
        )

    if not request.matches:
        raise ValueError("matches must contain at least one matching key")

    for matchingKey, distance in request.matches.items():
        if not SHA_256_PATTERN.fullmatch(matchingKey):
            raise ValueError(
                "every matching key must be a lowercase 64-character "
                "SHA-256 key"
            )

        if matchingKey == request.sourceSentenceKey:
            raise ValueError("a sentence key cannot match itself")

        if isinstance(distance, bool) or not isinstance(distance, (int, float)):
            raise ValueError("every distance must be a number")

        if distance < 0 or distance > 2:
            raise ValueError("every distance must be between 0 and 2")


def readSqlFunctionSourceMatches(session, request):
    """Read the source sentence's saved matches for the response."""

    rows = session.execute(
        text(
            """
            SELECT
                saved.match_bucket_number,
                item.matching_key,
                (item.distance_value #>> '{}')::DOUBLE PRECISION AS distance
            FROM sentence_key_matches AS saved
            CROSS JOIN LATERAL jsonb_each(saved.matches)
                AS item(matching_key, distance_value)
            WHERE saved.analysis_group = :analysisGroup
              AND saved.sentence_key = :sourceSentenceKey
            ORDER BY distance, item.matching_key
            """
        ),
        {
            "analysisGroup": request.analysisGroup,
            "sourceSentenceKey": request.sourceSentenceKey,
        },
    ).mappings().all()

    sourceMatches = []

    for row in rows:
        sourceMatches.append(
            {
                "matchingSentenceKey": row["matching_key"],
                "distance": float(row["distance"]),
                "bucketNumber": int(row["match_bucket_number"]),
            }
        )

    return sourceMatches


def readSqlFunctionReverseMatches(session, request):
    """Read all reverse matches for the response in one database call."""

    statement = text(
        """
        SELECT
            sentence_key,
            match_bucket_number,
            (
                matches ->> CAST(:sourceSentenceKey AS TEXT)
            )::DOUBLE PRECISION AS distance
        FROM sentence_key_matches
        WHERE analysis_group = :analysisGroup
          AND sentence_key IN :matchingKeys
          AND match_bucket_number = sentence_match_bucket(
              :sourceSentenceKey
          )
          AND (
              matches -> CAST(:sourceSentenceKey AS TEXT)
          ) IS NOT NULL
        ORDER BY distance, sentence_key
        """
    ).bindparams(
        bindparam("matchingKeys", expanding=True)
    )

    rows = session.execute(
        statement,
        {
            "analysisGroup": request.analysisGroup,
            "sourceSentenceKey": request.sourceSentenceKey,
            "matchingKeys": list(request.matches),
        },
    ).mappings().all()

    reverseMatches = []

    for row in rows:
        reverseMatches.append(
            {
                "sentenceKey": row["sentence_key"],
                "matchingSentenceKey": request.sourceSentenceKey,
                "distance": float(row["distance"]),
                "bucketNumber": int(row["match_bucket_number"]),
            }
        )

    return reverseMatches


def insertMatchesWithSqlFunction(session, request):
    """Run the complete PostgreSQL-function endpoint flow."""

    validateSqlFunctionRequest(request)
    print({
                "analysisGroup": request.analysisGroup,
                "sourceSentenceKey": request.sourceSentenceKey,
                "matches": request.matches,
            })

    processedCount = session.execute(
        SQL_FUNCTION_INSERT,
        {
            "analysisGroup": request.analysisGroup,
            "sourceSentenceKey": request.sourceSentenceKey,
            "matches": request.matches,
        },
    ).scalar_one()

    session.commit()

    sourceMatches = readSqlFunctionSourceMatches(session, request)
    reverseMatches = readSqlFunctionReverseMatches(session, request)

    return {
        "method": "sqlFunction",
        "analysisGroup": request.analysisGroup,
        "processedRelationships": processedCount,
        "sourceSentenceKey": request.sourceSentenceKey,
        "sourceMatches": sourceMatches,
        "reverseMatches": reverseMatches,
    }
