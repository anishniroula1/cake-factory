from sentence_search.models import SentenceOccurrence
from sentence_search.sentence_service import SentenceService


class FakeOpenSearchStore:
    def __init__(self):
        self.catalog_documents = []
        self.occurrence_documents = []

    def existing_catalog_keys(self, keys):
        return set()

    def bulk_index_catalog(self, documents):
        self.catalog_documents.extend(documents)

    def bulk_index_occurrences(self, documents):
        self.occurrence_documents.extend(documents)

    def refresh_indices(self):
        return None


def test_tracer_gets_vector_and_occurrence_without_matching_work():
    opensearch = FakeOpenSearchStore()
    service = SentenceService(opensearch)
    sentence = SentenceOccurrence.model_validate(
        {
            "applicationId": "A1",
            "tspId": "T1",
            "sectionName": "Statement",
            "globalId": 1,
            "sentIdLocal": 1,
            "sentenceContent": "Standard tracer text",
            "isTracer": True,
            "isFormLanguage": False,
            "sourceType": "document",
            "analysisGroup": "Asylee",
        }
    )

    result = service.add_sentence(sentence)

    assert len(opensearch.catalog_documents) == 1
    assert len(opensearch.occurrence_documents) == 1
    assert result["catalogDocumentIndexed"] is True
    assert result["occurrenceIndexed"] is True
    assert result["status"] == "completed"
    assert "matchCalculated" not in result
