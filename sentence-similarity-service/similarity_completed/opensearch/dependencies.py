from sqlalchemy.orm import sessionmaker

from similarity_completed.opensearch.config import Config
from similarity_completed.opensearch.handlers.sentence_similarity_handler import SentenceSimilarityHandler
from similarity_completed.opensearch.open_search_client import OpenSearchClient
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class Dependencies:
    """Build and own the shared resources required by the SQS processor."""

    def __init__(
        self, custom_logger: CustomLogger | None = None, database_connection: DatabaseConnection | None = None
    ) -> None:
        self.logger = custom_logger or CustomLogger("sentence_similarity")
        self.config = Config.from_environment()
        self.database = database_connection or self._create_database_connection()
        self.session_factory = sessionmaker(bind=self.database.engine, autoflush=False, expire_on_commit=False)

        self.opensearch_client = OpenSearchClient(self.config).create_client()
        self.aws_sqs = AwsSQS(
            queue_url=self.config.receive_sqs_endpoint,
            aws_region=self.config.aws_region,
            aws_endpoint_url=self.config.aws_local_endpoint_url or None,
            max_number_of_messages=1,
            wait_time=self.config.sqs_wait_seconds,
            logger=self.logger,
            visibility_timeout=self.config.sqs_visibility_seconds,
        )
        self.sentence_similarity_handler = SentenceSimilarityHandler(
            self.config, self.session_factory, self.opensearch_client, self.aws_sqs, self.logger
        )

    def close(self) -> None:
        """Close the database pool when the process stops."""

        self.database.engine.dispose()

    def _create_database_connection(self) -> DatabaseConnection:
        """Create the database connection from Vault-backed configuration."""

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
            application_name="sentence_similarity",
            **credential_arguments,
        )
