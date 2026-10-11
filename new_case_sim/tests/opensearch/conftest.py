from dataclasses import replace

import pytest

from case_similarity_service.opensearch.config import Config


@pytest.fixture
def service_config() -> Config:
    return Config(
        pg_db_user="user",
        pg_db_password="password",
        pg_db_host="database-local",
        pg_db_database="case-similarity-local",
        pg_db_port=5432,
        database_pool_size=20,
        database_max_overflow=10,
        dynamic_credential_file="",
        environment="local",
        aws_region="us-east-1",
        aws_local_endpoint_url="http://aws-local:4566",
        receive_sqs_endpoint="http://aws-local:4566/case-similarity-local.fifo",
        sent_sqs_endpoint="http://aws-local:4566/next-local.fifo",
        sqs_wait_seconds=10,
        sqs_visibility_seconds=7200,
        kafka_bootstrap_servers="kafka-local:9092",
        kafka_security_protocol="PLAINTEXT",
        kafka_environment="local",
        kafka_server_ca_cert="",
        ssl_cert_file="",
        ssl_key_file="",
        tah_topic="rfs.ata.text_analytics.hit",
        application_url="https://ui.example/case/",
        send_tah_kafka_events=True,
        min_similar_case_count=2,
        min_tah_case_count=3,
        retry_seconds=0,
        max_attempts=3,
    )


@pytest.fixture
def one_attempt_config(service_config: Config) -> Config:
    return replace(service_config, max_attempts=1)
