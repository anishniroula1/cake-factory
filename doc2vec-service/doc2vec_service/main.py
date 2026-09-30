from doc2vec_service.contracts import Logger
from doc2vec_service.DatabaseConnection import DatabaseConnection
from doc2vec_service.dependencies import Dependencies
from doc2vec_service.sqs_processor import SqsProcessor


def run(
    customLogger: Logger | None = None,
    databaseConnection: DatabaseConnection | None = None,
    dependencies: Dependencies | None = None,
) -> None:
    """Build one dependency container and start the SQS processor."""

    serviceDependencies = dependencies or Dependencies(
        customLogger,
        databaseConnection,
    )
    processor = SqsProcessor(serviceDependencies)

    try:
        processor.run()
    finally:
        serviceDependencies.close()


if __name__ == "__main__":
    run()
