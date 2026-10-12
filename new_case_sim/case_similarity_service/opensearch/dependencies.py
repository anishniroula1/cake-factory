from confluent_kafka import Producer
from sqlalchemy.orm import sessionmaker

from case_similarity_service.opensearch.config import Config
from case_similarity_service.opensearch.db_store.case_similarity_db_store import CaseSimilarityDBStore
from case_similarity_service.opensearch.db_store.kafka_event_db_store import KafkaEventDBStore
from case_similarity_service.opensearch.handlers.case_similarity_handler import CaseSimilarityHandler
from case_similarity_service.opensearch.kafka_processor import KafkaProcessor
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class Dependencies:
    def __init__(
        self,
        custom_logger: CustomLogger | None = None,
        database_connection: DatabaseConnection | None = None,
        config: Config | None = None,
    ) -> None:
        """Create the objects shared while one SQS message is processed."""

        self.logger = custom_logger or CustomLogger("case_similarity")
        self.config = config or Config.from_environment()
        self.database = database_connection or self._create_database_connection()
        self.session_factory = sessionmaker(bind=self.database.engine, autoflush=False, expire_on_commit=False)
        self.aws_sqs = AwsSQS(
            queue_url=self.config.receive_sqs_endpoint,
            aws_region=self.config.aws_region,
            aws_endpoint_url=self.config.aws_local_endpoint_url or None,
            max_number_of_messages=1,
            wait_time=self.config.sqs_wait_seconds,
            logger=self.logger,
            visibility_timeout=self.config.sqs_visibility_seconds,
        )
        self.case_similarity_db_store = CaseSimilarityDBStore(self.config, self.session_factory, self.logger)
        self.kafka_event_db_store = KafkaEventDBStore(self.session_factory)
        # Historical deployments keep Kafka disabled, so they do not create a
        # broker connection or require Kafka credentials.
        self.kafka_producer = Producer(self._kafka_config()) if self.config.send_tah_kafka_events else None
        self.kafka_processor = KafkaProcessor(self.config, self.kafka_producer, self.kafka_event_db_store, self.logger)
        self.case_similarity_handler = CaseSimilarityHandler(
            self.config, self.case_similarity_db_store, self.kafka_processor, self.aws_sqs, self.logger
        )

    def close(self) -> None:
        """Close the PostgreSQL pool when the worker stops."""

        self.database.engine.dispose()

    def _create_database_connection(self) -> DatabaseConnection:
        """Create the shared database library from Vault-backed values."""

        credential_arguments = (
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
            application_name="case_similarity",
            **credential_arguments,
        )

    def _kafka_config(self) -> dict:
        """Build the settings used by the Confluent Kafka producer."""

        values = {
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
            "security.protocol": self.config.kafka_security_protocol,
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
