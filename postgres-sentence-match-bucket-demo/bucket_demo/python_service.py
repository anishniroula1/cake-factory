import json
import re

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import SmallInteger
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import bindparam
from sqlalchemy import case
from sqlalchemy import func
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert


MATCH_BUCKET_COUNT = 32
SHA_256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

pythonMetadata = MetaData()

# This local table definition is used only to build the Python-side upsert.
pythonSentenceKeyMatches = Table(
    "sentence_key_matches",
    pythonMetadata,
    Column("analysis_group", Text, primary_key=True),
    Column("sentence_key", Text, primary_key=True),
    Column("match_bucket_number", SmallInteger, primary_key=True),
    Column("matches", JSONB, nullable=False),
    Column("match_key_count", Integer),
    Column("created_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
)


def pythonMatchBucket(sentenceKeyValue):
    """Calculate the bucket from the first byte of a SHA-256 key."""

    return int(sentenceKeyValue[:2], 16) % MATCH_BUCKET_COUNT


def validatePythonRequest(request):
    """Validate values used by the Python and SQLAlchemy endpoint."""

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


def buildPythonBucketRows(request):
    """Create all forward and reverse bucket patches in Python."""

    validatePythonRequest(request)
    bucketPatches = {}
    sourceBucket = pythonMatchBucket(request.sourceSentenceKey)

    for matchingKey, distance in request.matches.items():
        forwardIdentity = (
            request.analysisGroup,
            request.sourceSentenceKey,
            pythonMatchBucket(matchingKey),
        )
        # print('forwardIdentity: ', forwardIdentity)
        reverseIdentity = (
            request.analysisGroup,
            matchingKey,
            sourceBucket,
        )
        # print('reverseIdentity: ', reverseIdentity)

        if forwardIdentity not in bucketPatches:
            bucketPatches[forwardIdentity] = {}

        if reverseIdentity not in bucketPatches:
            bucketPatches[reverseIdentity] = {}

        bucketPatches[forwardIdentity][matchingKey] = distance
        bucketPatches[reverseIdentity][request.sourceSentenceKey] = distance

    # print(bucketPatches)
    bucketRows = []

    # Every transaction updates rows in the same order to reduce deadlocks.
    for identity in sorted(bucketPatches):
        analysisGroup, ownerKey, bucketNumber = identity
        bucketRows.append(
            {
                "analysisGroup": analysisGroup,
                "sentenceKey": ownerKey,
                "matchBucketNumber": bucketNumber,
                "incomingMatches": bucketPatches[identity],
            }
        )
    # print(json.dumps(bucketRows, indent=4))
    return bucketRows


def buildPythonUpsertStatement():
    """Build the Python endpoint's bind-parameter bulk upsert."""

    baseInsert = insert(pythonSentenceKeyMatches).values(
        analysis_group=bindparam("analysisGroup"),
        sentence_key=bindparam("sentenceKey"),
        match_bucket_number=bindparam("matchBucketNumber"),
        matches=bindparam("incomingMatches", type_=JSONB),
    )

    return baseInsert.on_conflict_do_update(
        index_elements=[
            pythonSentenceKeyMatches.c.analysis_group,
            pythonSentenceKeyMatches.c.sentence_key,
            pythonSentenceKeyMatches.c.match_bucket_number,
        ],
        # This section runs when the bucket row already exists.
        #
        # If the current JSONB object already contains every incoming key with
        # the same distance, keep the current object unchanged.
        #
        # Otherwise, the PostgreSQL JSONB || operator merges the incoming
        # object into the current object. New matching keys are added. When a
        # matching key already exists with a different distance, the incoming
        # distance replaces the old distance. Because matches is a JSONB
        # object, the same matching key cannot be stored twice.
        #
        # updated_at records when this upsert was processed.
        set_={
            "matches": case(
                (
                    pythonSentenceKeyMatches.c.matches.contains(
                        baseInsert.excluded.matches
                    ),
                    pythonSentenceKeyMatches.c.matches,
                ),
                else_=(
                    pythonSentenceKeyMatches.c.matches
                    .op("||")(baseInsert.excluded.matches)
                ),
            ),
            "updated_at": func.now(),
        },
    )


def readPythonSourceMatches(session, request):
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


def readPythonReverseMatches(session, request):
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
          AND match_bucket_number = :sourceBucket
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
            "sourceBucket": pythonMatchBucket(request.sourceSentenceKey),
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


def insertMatchesWithPython(session, request):
    """Run the complete Python and SQLAlchemy endpoint flow."""

    bucketRows = buildPythonBucketRows(request)
    upsertStatement = buildPythonUpsertStatement()

    session.execute(upsertStatement, bucketRows)
    session.commit()

    sourceMatches = readPythonSourceMatches(session, request)
    reverseMatches = readPythonReverseMatches(session, request)

    return {
        "method": "pythonBindParameters",
        "analysisGroup": request.analysisGroup,
        "processedRelationships": len(request.matches),
        "sourceSentenceKey": request.sourceSentenceKey,
        "sourceMatches": sourceMatches,
        "reverseMatches": reverseMatches,
    }
