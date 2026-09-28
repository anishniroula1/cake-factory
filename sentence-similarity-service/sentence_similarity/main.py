import logging

import boto3

from sentence_similarity.config import Config
from sentence_similarity.database import Database
from sentence_similarity.handlers.sentence_similarity_handler import (
    SentenceSimilarityHandler,
)
from sentence_similarity.open_search_client import OpenSearchClient
from sentence_similarity.opensearch import OpenSearchStore
from sentence_similarity.sqs_listener import SqsListener

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def run():
    """Create the application and start listening for tsp_id messages."""

    config = Config.fromEnvironment()
    database = Database(config)
    awsSession = boto3.Session(region_name=config.awsRegion)
    sqsClient = awsSession.client("sqs", region_name=config.awsRegion)
    opensearchClient = OpenSearchClient(config).create_client()
    opensearch = OpenSearchStore(config, opensearchClient)
    handler = SentenceSimilarityHandler(
        config,
        database.sessionFactory,
        opensearch,
        sqsClient,
    )
    listener = SqsListener(config, sqsClient, handler)

    try:
        listener.run()
    finally:
        database.close()


if __name__ == "__main__":
    run()
