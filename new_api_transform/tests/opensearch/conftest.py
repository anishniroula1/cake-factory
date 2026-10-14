import pytest

from api_transform_service.opensearch import constants
from api_transform_service.opensearch.config import Config


@pytest.fixture
def service_config(monkeypatch) -> Config:
    monkeypatch.setattr(constants, "RETRY_SECONDS", 0)
    return Config(
        pg_db_user="postgres",
        pg_db_password="postgres",
        pg_db_host="localhost",
        pg_db_database="postgres",
        pg_db_port=5432,
        dynamic_credential_file="",
        environment="local",
        aws_region="us-east-1",
        aws_local_endpoint_url="http://localhost:4566",
        receive_sqs_endpoint="http://localhost/source-local.fifo",
        dlq_sqs_endpoint="http://localhost/source-dlq-local.fifo",
        sent_sqs_endpoint="http://localhost/next-local.fifo",
        kafka_bootstrap_servers="localhost:9092",
        kafka_security_protocol="PLAINTEXT",
        kafka_environment="local",
        kafka_server_ca_cert="",
        ssl_cert_file="",
        ssl_key_file="",
        kafka_compression="lz4",
        verbose_kafka_logging=False,
        analysis_group="Asylee",
        reduce_kafka_size=False,
    )


@pytest.fixture
def tiny_payload_config(service_config: Config, monkeypatch) -> Config:
    monkeypatch.setattr(constants, "COMPRESSED_MAX_EVENT_BYTES", 800)
    return service_config
