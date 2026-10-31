from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.contracts import OccurrenceDocument
from doc2vec_completed.opensearch.opensearch_helper import OpenSearchHelper
from ta_db_connection.atatables import SentencesVector


class SentenceOccurrenceOpenSearchStore:
    """Write sentence occurrence documents to their configured alias."""

    def __init__(
        self,
        opensearch_helper: OpenSearchHelper,
    ) -> None:
        self.alias = constants.SENTENCE_OCCURRENCE_ALIAS
        self.opensearch_helper = opensearch_helper

    def index_occurrences(self, documents: list[OccurrenceDocument]) -> int:
        """Index occurrence records without running an embedding pipeline.

        Input: documents=[{"_id": 10, "globalId": 10, ...}]
        Output: the number of occurrence documents accepted by OpenSearch.
        """

        return self.opensearch_helper.index_documents(
            self.alias,
            documents,
            needs_embedding=False,
        )

    @staticmethod
    def build_documents(
        sentence_vector_records: list[SentencesVector],
    ) -> list[OccurrenceDocument]:
        """Build one occurrence document for every saved sentence record.

        Input: two SentencesVector records owned by application 1001.
        Output: two occurrence documents containing their global IDs, sentence
        keys, document location, and analysis group.

        Sentence text stays in the catalog. The occurrence keeps only the
        identity and location fields needed to find that catalog record.
        """

        document = sentence_vector_records[0].tsp
        return [
            {
                "_id": sentence_vector_record.global_id,
                "globalId": sentence_vector_record.global_id,
                "applicationId": document.application_id,
                "tspId": document.tsp_id,
                "sectionName": document.section_name,
                "sentIdLocal": sentence_vector_record.sent_id_local,
                "sentenceKey": sentence_vector_record.sentence_key,
                "sourceType": sentence_vector_record.source_type,
                "analysisGroup": document.analysis_group,
                "isCurrent": document.is_current,
                "createdAt": sentence_vector_record.last_modified.isoformat(),
                "updatedAt": sentence_vector_record.last_modified.isoformat(),
            }
            for sentence_vector_record in sentence_vector_records
        ]

