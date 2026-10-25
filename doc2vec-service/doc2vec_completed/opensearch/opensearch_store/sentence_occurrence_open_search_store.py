from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import OccurrenceDocument
from doc2vec_completed.opensearch.open_search_helper import OpenSearchHelper
from ta_db_connection.atables import SentencesVector


class SentenceOccurrenceOpenSearchStore:
    """Write sentence occurrence documents to their configured alias."""

    def __init__(
        self,
        config: Config,
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
    def build_documents(rows: list[SentencesVector]) -> list[OccurrenceDocument]:
        """Build one occurrence document for every saved sentence row.

        Sentence text stays in the catalog. The occurrence keeps only the
        identity and location fields needed to find that catalog record.
        """

        document = rows[0].tsp
        return [
            {
                "_id": row.global_id,
                "globalId": row.global_id,
                "applicationId": document.application_id,
                "tspId": document.tsp_id,
                "sectionName": document.section_name,
                "sentIdLocal": row.sent_id_local,
                "sentenceKey": row.sentence_key,
                "sourceType": row.source_type,
                "analysisGroup": document.analysis_group,
                "isCurrent": document.is_current,
                "createdAt": row.last_modified.isoformat(),
                "updatedAt": row.last_modified.isoformat(),
            }
            for row in rows
        ]
