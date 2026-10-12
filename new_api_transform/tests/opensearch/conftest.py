from dataclasses import replace

import pytest

from api_transform_service.opensearch.config import Config


@pytest.fixture
def service_config() -> Config:
    return Config(
        pg_db_user="postgres",
        pg_db_password="postgres",
        pg_db_host="localhost",
        pg_db_database="postgres",
        pg_db_port=5432,
        database_pool_size=20,
        database_max_overflow=10,
        dynamic_credential_file="",
        environment="local",
        aws_region="us-east-1",
        aws_local_endpoint_url="http://localhost:4566",
        receive_sqs_endpoint="http://localhost/source-local.fifo",
        dlq_sqs_endpoint="http://localhost/source-dlq-local.fifo",
        sent_sqs_endpoint="http://localhost/next-local.fifo",
        sqs_wait_seconds=10,
        sqs_visibility_seconds=7200,
        kafka_bootstrap_servers="localhost:9092",
        kafka_security_protocol="PLAINTEXT",
        kafka_environment="local",
        kafka_server_ca_cert="",
        ssl_cert_file="",
        ssl_key_file="",
        kafka_compression="lz4",
        kafka_topic="rfs.ata.data_migration.api.events",
        verbose_kafka_logging=False,
        analysis_group="Asylee",
        reduce_kafka_size=False,
        sentence_match_limit=3000,
        uncompressed_max_event_bytes=4718592,
        compressed_max_event_bytes=18155776,
        kafka_buffer_limit_bytes=450000000,
        kafka_audit_retention_days=60,
        retry_seconds=0,
        max_attempts=3,
    )


@pytest.fixture
def tiny_payload_config(service_config: Config) -> Config:
    return replace(service_config, compressed_max_event_bytes=800)
