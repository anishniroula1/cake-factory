from doc_delete_completed.opensearch.dependencies import Dependencies
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.logger.CustomLogger import CustomLogger


def run_document_delete_job(
    custom_logger: CustomLogger | None = None,
    database_connection: DatabaseConnection | None = None,
    dependencies: Dependencies | None = None,
) -> None:
    """Build dependencies and run one database-driven deletion job."""

    service_dependencies = dependencies or Dependencies(custom_logger, database_connection)
    try:
        service_dependencies.document_delete_job.run()
    finally:
        service_dependencies.logger.set_generic_message(None)
        service_dependencies.close()


if __name__ == "__main__":
    run_document_delete_job()
