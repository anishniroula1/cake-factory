import pytest

from case_similarity_completed.opensearch import constants
from case_similarity_completed.opensearch.config import Config


@pytest.fixture
def service_config(monkeypatch) -> Config:
    monkeypatch.setattr(constants, "RETRY_SECONDS", 0)
    monkeypatch.setattr(constants, "MAX_ATTEMPTS", 1)
    return Config(
        pg_db_user="user",
        pg_db_password="password",
        pg_db_host="database-local",
        pg_db_database="case-similarity-local",
        pg_db_port=5432,
        dynamic_credential_file="",
        environment="local",
        aws_region="us-east-1",
        aws_local_endpoint_url="http://aws-local:4566",
        receive_sqs_endpoint="http://aws-local:4566/case-similarity-local.fifo",
        sent_sqs_endpoint="http://aws-local:4566/next-local.fifo",
        kafka_bootstrap_servers="kafka-local:9092",
        kafka_security_protocol="PLAINTEXT",
        kafka_environment="local",
        kafka_server_ca_cert="",
        ssl_cert_file="",
        ssl_key_file="",
        application_url="https://ui.example/case/",
        send_tah_kafka_events=True,
    )
