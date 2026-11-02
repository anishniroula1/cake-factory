from doc_delete_job.opensearch.contracts import TspId
from doc_delete_job.opensearch.dependencies import Dependencies
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.logger.CustomLogger import CustomLogger


def run_document_delete_job(
    tsp_ids: list[TspId],
    custom_logger: CustomLogger | None = None,
    database_connection: DatabaseConnection | None = None,
    dependencies: Dependencies | None = None,
) -> int:
    """Build dependencies and delete the supplied document IDs."""

    service_dependencies = dependencies or Dependencies(custom_logger, database_connection)
    try:
        return service_dependencies.document_delete_job.run(tsp_ids)
    finally:
        service_dependencies.logger.set_generic_message(None)
        service_dependencies.close()
