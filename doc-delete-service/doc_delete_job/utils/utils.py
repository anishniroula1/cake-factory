import os

from doc_delete_job.utils.doc_delete_config import get_config
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger

logger = CustomLogger("manage-faiss-service")

def create_default_sqs_client(queue_url: str):
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    endpoint_url = os.getenv("AWS_ENDPOINT_URL", None)
    max_number_of_messages = int(os.getenv("AWS_SQS_MAX_NUMBER_MESSAGE_TO_PROCESS", "10"))
    wait_time = int(os.getenv("AWS_SQS_WAIT_TIME_FOR_NEXT_MESSAGE_BATCH", "15"))

    visibility_timeout = int(os.getenv("AWS_SQS_VISIBILITY_TIMEOUT", "600"))
    local_logger = logger if isinstance(logger, CustomLogger) else None
    return AwsSQS(
        queue_url,
        aws_region,
        endpoint_url,
        max_number_of_messages,
        wait_time,
        logger=local_logger,
        visibility_timeout=visibility_timeout,
    )


def get_config_params():
    return get_config()["params"]


def get_manage_faiss_path():
    return get_config()["local_paths"]
