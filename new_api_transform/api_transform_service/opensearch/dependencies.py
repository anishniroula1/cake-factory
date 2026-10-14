from confluent_kafka import Producer
from sqlalchemy.orm import sessionmaker

from api_transform_service.opensearch.config import Config
from api_transform_service.opensearch.db_store import (
    ApplicantDBStore,
    ApplicationDBStore,
    ContentDBStore,
    TransmissionDBStore,
)
from api_transform_service.opensearch.handlers import ApiTransformHandler
from api_transform_service.opensearch.kafka_processor import KafkaProcessor
from api_transform_service.opensearch.payload_builder import PayloadBuilder
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


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

        self.aws_sqs = self._sqs(self.config.receive_sqs_endpoint)
        self.dlq_sqs = (
            self._sqs(self.config.dlq_sqs_endpoint, max_messages=10) if self.config.dlq_sqs_endpoint else None
        )
        self.sent_sqs = self._sqs(self.config.sent_sqs_endpoint)
        application_store = ApplicationDBStore(self.session_factory)
        applicant_store = ApplicantDBStore(self.session_factory)
        content_store = ContentDBStore(self.session_factory, self.config.sentence_match_limit)
        transmission_store = TransmissionDBStore(self.session_factory, self.config.kafka_environment)
        payload_builder = PayloadBuilder(self.config)
        kafka_processor = KafkaProcessor(self.config, Producer(self._kafka_config()), transmission_store, self.logger)
        self.api_transform_handler = ApiTransformHandler(
            self.config,
            application_store,
            applicant_store,
            content_store,
            transmission_store,
            payload_builder,
            kafka_processor,
            self.logger,
        )

    def close(self) -> None:
        """Close the PostgreSQL connection pool when the worker stops."""

        self.database.engine.dispose()

    def _create_database_connection(self) -> DatabaseConnection:
        """Create the shared database library from the configured PostgreSQL values."""

        credentials = (
            {"use_dynamic_credentials": False}
            if self.config.environment == "local"
            else {"dynamic_credentials_file": self.config.dynamic_credential_file}
        )
        return DatabaseConnection(
            pg_db_user=self.config.pg_db_user,
            pg_db_password=self.config.pg_db_password,
            pg_db_host=self.config.pg_db_host,
            pg_db_database=self.config.pg_db_database,
            pg_db_port=self.config.pg_db_port,
            pool_size=self.config.database_pool_size,
            max_overflow=self.config.database_max_overflow,
            application_name="api_transform",
            **credentials,
        )

    def _sqs(self, queue_url: str, max_messages: int = 1) -> AwsSQS:
        """Create an SQS client that uses the service's common AWS settings.

        Input: queue_url="http://localhost:4566/queue/api-transform-local" and max_messages=1.
        Output: an AwsSQS client configured to read one message from that queue.
        """

        return AwsSQS(
            queue_url=queue_url,
            aws_region=self.config.aws_region,
            aws_endpoint_url=self.config.aws_local_endpoint_url or None,
            max_number_of_messages=max_messages,
            wait_time=self.config.sqs_wait_seconds,
            logger=self.logger,
            visibility_timeout=self.config.sqs_visibility_seconds,
        )

    def _kafka_config(self) -> dict:
        """Build the Confluent producer settings used to publish API events.

        Input: the service configuration with kafka_security_protocol="PLAINTEXT".
        Output: producer settings containing the brokers, protocol, compression, and message-size limit.
        """

        values = {
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
            "security.protocol": self.config.kafka_security_protocol,
            "message.max.bytes": 100 * 1024 * 1024,
            "compression.type": self.config.kafka_compression,
        }
        if self.config.kafka_security_protocol == "SSL":
            values.update(
                {
                    "ssl.certificate.location": self.config.ssl_cert_file,
                    "ssl.key.location": self.config.ssl_key_file,
                    "ssl.ca.location": self.config.kafka_server_ca_cert,
                }
            )
        return values
