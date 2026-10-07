from sentence_similarity.custom_logger import CustomLogger
from sentence_similarity.DatabaseConnection import DatabaseConnection
from sentence_similarity.dependencies import Dependencies
from sentence_similarity.sqs_processor import SqsProcessor


def run_opensearch_flow(
    custom_logger: CustomLogger | None = None,
    database_connection: DatabaseConnection | None = None,
    dependencies: Dependencies | None = None,
) -> None:
    """Build one dependency container and start the SQS processor."""

    service_dependencies = dependencies or Dependencies(
        custom_logger,
        database_connection,
    )
    processor = SqsProcessor(service_dependencies)

    try:
        processor.run()
    finally:
        service_dependencies.close()


if __name__ == "__main__":
    run_opensearch_flow()
