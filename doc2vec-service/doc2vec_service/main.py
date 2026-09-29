import logging

import boto3

from doc2vec_service.config import Config
from doc2vec_service.database import Database
from doc2vec_service.handlers.historical_handler import HistoricalHandler
from doc2vec_service.handlers.live_handler import LiveHandler
from doc2vec_service.handlers.tracer_handler import TracerHandler
from doc2vec_service.handlers.train_handler import TrainHandler
from doc2vec_service.open_search_client import OpenSearchClient
from doc2vec_service.opensearch import OpenSearchHelper
from doc2vec_service.sqs_listener import SqsListener

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def run():
    """Create the application and start listening for SQS messages."""

    config = Config.fromEnvironment()
    awsSession = boto3.Session(region_name=config.awsRegion)
    database = Database(config)
    opensearchClient = OpenSearchClient(config).create_client()
    opensearch = OpenSearchHelper(config, opensearchClient)
    sqsClient = awsSession.client("sqs", region_name=config.awsRegion)

    trainHandler = TrainHandler(config, database.sessionFactory, opensearch)
    tracerHandler = TracerHandler(config, database.sessionFactory, opensearch)
    historicalHandler = HistoricalHandler(
        config,
        database.sessionFactory,
        opensearch,
        sqsClient,
    )
    liveHandler = LiveHandler(
        config,
        database.sessionFactory,
        opensearch,
        sqsClient,
    )
    listener = SqsListener(
        config,
        sqsClient,
        trainHandler,
        tracerHandler,
        historicalHandler,
        liveHandler,
    )

    try:
        listener.run()
    finally:
        database.close()


if __name__ == "__main__":
    run()
