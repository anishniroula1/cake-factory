from api_transform_service.opensearch.dependencies import Dependencies
from api_transform_service.opensearch.sqs_processor import SqsProcessor
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.logger.CustomLogger import CustomLogger


def run_opensearch_flow(
    custom_logger: CustomLogger | None = None,
    database_connection: DatabaseConnection | None = None,
    dependencies: Dependencies | None = None,
) -> None:
    """Build dependencies and start the API Transform SQS processor.

    Input: optional test or application-owned logger, database connection, or dependencies.
    The database pool and generic log context are cleaned up when the processor stops.
    """

    service_dependencies = dependencies or Dependencies(custom_logger, database_connection)
    try:
        SqsProcessor(service_dependencies).run()
    finally:
        service_dependencies.logger.set_generic_message(None)
        service_dependencies.close()


if __name__ == "__main__":
    run_opensearch_flow()
