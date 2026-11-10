from confluent_kafka import Producer
from sqlalchemy.orm import sessionmaker

from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger
from transform_completed.opensearch import constants
from transform_completed.opensearch.config import Config
from transform_completed.opensearch.db_store import (
    ApplicantDBStore,
    ApplicationDBStore,
    ContentDBStore,
    TransmissionDBStore,
)
from transform_completed.opensearch.handlers import ApiTransformHandler
from transform_completed.opensearch.kafka_processor import KafkaProcessor
from transform_completed.opensearch.payload_builder import PayloadBuilder


class Dependencies:
    """Create and hold the shared clients, stores, and processors used by the worker."""

    def __init__(
        self,
        custom_logger: CustomLogger | None = None,
        database_connection: DatabaseConnection | None = None,
        config: Config | None = None,
    ) -> None:
        self.logger = custom_logger or CustomLogger("api_transform")
        self.config = config or Config.from_environment()
        self.database = database_connection or self._create_database_connection()
        self.session_factory = sessionmaker(bind=self.database.engine, autoflush=False, expire_on_commit=False)

        self.aws_sqs = self._create_sqs_client(self.config.receive_sqs_endpoint)
        self.dlq_sqs = (
            self._create_sqs_client(self.config.dlq_sqs_endpoint, max_messages=10)
            if self.config.dlq_sqs_endpoint
            else None
        )
        self.sent_sqs = self._create_sqs_client(self.config.sent_sqs_endpoint)
        application_db_store = ApplicationDBStore(self.session_factory)
        applicant_db_store = ApplicantDBStore(self.session_factory)
        content_db_store = ContentDBStore(self.session_factory)
        transmission_db_store = TransmissionDBStore(self.session_factory, self.config.kafka_environment)
        payload_builder = PayloadBuilder(self.config)
        kafka_processor = KafkaProcessor(
            self.config, Producer(self._build_kafka_producer_config()), transmission_db_store, self.logger
        )
        self.api_transform_handler = ApiTransformHandler(
            self.config,
            application_db_store,
            applicant_db_store,
            content_db_store,
            transmission_db_store,
            payload_builder,
            kafka_processor,
            self.logger,
        )

    def close(self) -> None:
        """Close the PostgreSQL connection pool when the worker stops."""

        self.database.engine.dispose()

    def _create_database_connection(self) -> DatabaseConnection:
        """Create the shared database library from the configured PostgreSQL values."""

        credential_arguments = (
            {"use_dynamic_credentials": False}
            if self.config.environment == "local"
            else {"use_dynamic_credentials": True, "dynamic_credentials_file": self.config.dynamic_credential_file}
        )
        return DatabaseConnection(
            pg_db_user=self.config.pg_db_user,
            pg_db_password=self.config.pg_db_password,
            pg_db_host=self.config.pg_db_host,
            pg_db_database=self.config.pg_db_database,
            pg_db_port=self.config.pg_db_port,
            pool_size=constants.DATABASE_POOL_SIZE,
            max_overflow=constants.DATABASE_MAX_OVERFLOW,
            application_name="api_transform",
            **credential_arguments,
        )

    def _create_sqs_client(self, queue_url: str, max_messages: int = 1) -> AwsSQS:
        """Create an SQS client that uses the service's common AWS settings.

        Input: queue_url="http://localhost:4566/queue/api-transform-local" and max_messages=1.
        Output: an AwsSQS client configured to read one message from that queue.
        """

        return AwsSQS(
            queue_url=queue_url,
            aws_region=self.config.aws_region,
            aws_endpoint_url=self.config.aws_local_endpoint_url or None,
            max_number_of_messages=max_messages,
            wait_time=constants.SQS_WAIT_SECONDS,
            logger=self.logger,
            visibility_timeout=constants.SQS_VISIBILITY_SECONDS,
        )

    def _build_kafka_producer_config(self) -> dict:
        """Build the Confluent producer settings used to publish API events.

        Input: the service configuration with kafka_security_protocol="PLAINTEXT".
        Output: producer settings containing the brokers, protocol, compression, and message-size limit.
        """

        kafka_producer_config = {
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
            "security.protocol": self.config.kafka_security_protocol,
            "message.max.bytes": constants.KAFKA_MESSAGE_MAX_BYTES,
            "compression.type": self.config.kafka_compression,
        }
        if self.config.kafka_security_protocol == "SSL":
            kafka_producer_config.update(
                {
                    "ssl.certificate.location": self.config.ssl_cert_file,
                    "ssl.key.location": self.config.ssl_key_file,
                    "ssl.ca.location": self.config.kafka_server_ca_cert,
                }
            )
        return kafka_producer_config
