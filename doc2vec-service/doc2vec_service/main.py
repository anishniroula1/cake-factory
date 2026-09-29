from doc2vec_service.dependencies import Dependencies
from doc2vec_service.sqs_processor import SqsProcessor


def run(customLogger=None, databaseConnection=None):
    """Build one dependency container and start the SQS processor."""

    dependencies = Dependencies(customLogger, databaseConnection)
    processor = SqsProcessor(dependencies)

    try:
        processor.run()
    finally:
        dependencies.close()


if __name__ == "__main__":
    run()
