import json
from dataclasses import replace

import boto3
import pytest
from moto import mock_aws

from similarity_completed.opensearch import main
from similarity_completed.opensearch.handlers.sentence_similarity_handler import SentenceSimilarityHandler
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger
from tests.opensearch.fakes import SessionSequence, opensearch_mock, session_mock


class StopApplication(Exception):
    pass


class OneMessageAwsSqs(AwsSQS):
    """Use Moto for one real SQS poll, then stop the worker loop."""

    def __init__(self, **values):
        super().__init__(**values)
        self.polls = 0

    def listen_sqs_message(self):
        self.polls += 1
        if self.polls == 1:
            return super().listen_sqs_message()
        raise StopApplication


class Dependencies:
    def __init__(self, queue, handler, custom_logger):
        self.aws_sqs = queue
        self.sentence_similarity_handler = handler
        self.logger = custom_logger
        self.closed = False

    def close(self):
        self.closed = True


@mock_aws
def test_main_runs_complete_unit_flow_with_mocked_service_boundaries(service_config, sentence_row):
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    source_queue_url = sqs_client.create_queue(QueueName="sentence-similarity-local")["QueueUrl"]
    next_queue_url = sqs_client.create_queue(
        QueueName="sentence-transform-local.fifo", Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"}
    )["QueueUrl"]
    sqs_client.send_message(QueueUrl=source_queue_url, MessageBody=json.dumps({"tsp_id": 10}))

    custom_logger = CustomLogger("sentence_similarity_unit_test")
    queue = OneMessageAwsSqs(
        queue_url=source_queue_url,
        aws_region="us-east-1",
        max_number_of_messages=1,
        wait_time=0,
        logger=custom_logger,
        visibility_timeout=0,
    )
    source_update = session_mock(rows=[100])
    target_update = session_mock(rows=[200])
    match_write = session_mock(execute_value=1)
    sessions = SessionSequence(
        [
            session_mock(scalar_value=1),
            session_mock(rows=[sentence_row]),
            source_update,
            target_update,
            match_write,
            session_mock(rows=[]),
        ]
    )
    opensearch, _ = opensearch_mock(sentence_row.sentence_key)
    config = replace(service_config, receive_sqs_endpoint=source_queue_url, sent_sqs_endpoint=next_queue_url)
    handler = SentenceSimilarityHandler(config, sessions, opensearch, queue, custom_logger)
    dependencies = Dependencies(queue, handler, custom_logger)

    with pytest.raises(StopApplication):
        main.run_opensearch_flow(dependencies=dependencies)

    assert "Messages" not in sqs_client.receive_message(QueueUrl=source_queue_url, WaitTimeSeconds=0)
    next_message = sqs_client.receive_message(QueueUrl=next_queue_url, WaitTimeSeconds=0)["Messages"][0]
    assert json.loads(next_message["Body"]) == {"tsp_id": 10}

    parameters = match_write.execute.call_args.args[1]
    assert parameters == {
        "source_global_id": 100,
        "source_tsp_id": 10,
        "source_application_id": 1,
        "matches": {"200": {"tsp_id": 20, "application_id": 2, "distance": 0.0}},
    }
