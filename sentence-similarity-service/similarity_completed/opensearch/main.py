from similarity_completed.opensearch.dependencies import Dependencies
from similarity_completed.opensearch.sqs_processor import SqsProcessor
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.logger.CustomLogger import CustomLogger


def run_opensearch_flow(
    custom_logger: CustomLogger | None = None,
    database_connection: DatabaseConnection | None = None,
    dependencies: Dependencies | None = None,
) -> None:
    """Build one dependency container and start the SQS processor."""

    service_dependencies = dependencies or Dependencies(custom_logger, database_connection)
    processor = SqsProcessor(service_dependencies)

    try:
        processor.run()
    finally:
        service_dependencies.close()


if __name__ == "__main__":
    run_opensearch_flow()
