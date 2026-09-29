import boto3

from doc2vec_service.config import Config
from doc2vec_service.custom_logger import CustomLogger
from doc2vec_service.database import Database
from doc2vec_service.handlers.historical_handler import HistoricalHandler
from doc2vec_service.open_search_client import OpenSearchClient
from doc2vec_service.opensearch import OpenSearchHelper
from doc2vec_service.sqs_listener import SqsListener


def run(customLogger=None):
    """Create the application and allow the team logger to be supplied."""

    logger = customLogger or CustomLogger("doc2vec_service")
    config = Config.fromEnvironment()
    awsSession = boto3.Session(region_name=config.awsRegion)
    database = Database(config)
    opensearchClient = OpenSearchClient(config).create_client()
    opensearch = OpenSearchHelper(config, opensearchClient, logger)
    sqsClient = awsSession.client("sqs", region_name=config.awsRegion)

    historicalHandler = HistoricalHandler(
        config,
        database.sessionFactory,
        opensearch,
        sqsClient,
        logger,
    )
    listener = SqsListener(
        config,
        sqsClient,
        historicalHandler,
        logger,
    )

    try:
        listener.run()
    finally:
        database.close()


if __name__ == "__main__":
    run()
