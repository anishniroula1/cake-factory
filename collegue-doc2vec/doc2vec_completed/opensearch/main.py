from doc2vec_completed.opensearch.dependencies import Dependencies
from doc2vec_completed.opensearch.sqs_processor import SqsProcessor
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.logger.CustomLogger import CustomLogger


def run_opensearch_flow(
    custom_logger: CustomLogger | None = None,
    database_connection: DatabaseConnection | None = None,
    dependencies: Dependencies | None = None,
) -> None:
    """Build service dependencies and start the Doc2Vec SQS processing loop.

    Input: optional custom_logger, database_connection, or prebuilt dependencies.

    When no objects are supplied, production dependencies are built from Vault
    and environment configuration. Resources close when the loop exits.
    """

    service_dependencies = dependencies or Dependencies(
        custom_logger,
        database_connection,
    )
    processor = SqsProcessor(service_dependencies)

    try:
        processor.run()
    finally:
        service_dependencies.logger.set_generic_message(None)
        service_dependencies.close()


if __name__ == "__main__":
    run_opensearch_flow()

