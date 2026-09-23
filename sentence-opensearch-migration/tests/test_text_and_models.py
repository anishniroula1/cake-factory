import pytest
from pydantic import ValidationError

from sentence_migration.config import config
from sentence_migration.models import SeedSentencesRequest, SentenceOccurrence
from sentence_migration.text import sentence_key

ROLE = "arn:aws:iam::123456789012:role/sentence-opensearch-primary"


def test_sentence_key_matches_reference_normalization():
    assert sentence_key("  Government OF the United States  ") == sentence_key(
        "government of the united states"
    )
    assert sentence_key("Let's eat, Grandma.") != sentence_key("Let's eat Grandma.")


def test_occurrence_generates_key_and_dates():
    record = SentenceOccurrence(
        applicationId="A1",
        tspId="T1",
        sectionName="Statement",
        globalId=12,
        sentIdLocal=3,
        sentenceContent="A sentence.",
        sourceType="document",
        analysisGroup="Asylee",
    )

    assert record.sentenceKey == sentence_key("A sentence.")
    assert record.createdAt is not None
    assert record.updatedAt is not None


def test_seed_request_defaults_to_first_environment_choices():
    request = SeedSentencesRequest(s3File="imports/tsp-ids.csv")

    assert request.iamRole == config.opensearch_iam_roles[0]
    assert request.occurrenceAlias == config.occurrence_aliases[0]
    assert request.catalogAlias == config.catalog_aliases[0]


def test_seed_request_accepts_only_environment_choices():
    request = SeedSentencesRequest(
        iamRole=ROLE,
        s3File="imports/tsp-ids.csv",
        occurrenceAlias="sentence_occurrences_test",
        catalogAlias="sentence_semantic_catalog_test",
    )
    assert request.iamRole == ROLE

    with pytest.raises(ValidationError, match="OPENSEARCH_IAM_ROLES"):
        SeedSentencesRequest(
            iamRole="arn:aws:iam::123456789012:role/not-configured",
            s3File="ids.csv",
        )

    with pytest.raises(ValidationError, match="OPENSEARCH_OCCURRENCE_ALIASES"):
        SeedSentencesRequest(
            s3File="ids.csv",
            occurrenceAlias="unlisted-alias",
        )
