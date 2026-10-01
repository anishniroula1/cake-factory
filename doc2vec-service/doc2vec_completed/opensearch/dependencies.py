from sqlalchemy.orm import sessionmaker

from doc2vec_completed.opensearch.AwsSQS import AwsSQS
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.contracts import Logger
from doc2vec_completed.opensearch.custom_logger import CustomLogger
from doc2vec_completed.opensearch.DatabaseConnection import DatabaseConnection
from doc2vec_completed.opensearch.handlers.historical_handler import HistoricalHandler
from doc2vec_completed.opensearch.open_search_client import OpenSearchClient
from doc2vec_completed.opensearch.opensearch import OpenSearchHelper


class Dependencies:
    def __init__(
        self,
        custom_logger: Logger | None = None,
        database_connection: DatabaseConnection | None = None,
    ) -> None:
        """Create everything used while processing an SQS message."""

        self.logger = custom_logger or CustomLogger("doc2vec_completed.opensearch")
        self.config = Config.from_environment()
        self.database = database_connection or self._create_database_connection()
        self.session_factory = sessionmaker(
            bind=self.database.engine,
            autoflush=False,
            expire_on_commit=False,
        )

        opensearch_client = OpenSearchClient(self.config).create_client()
        self.opensearch = OpenSearchHelper(
            self.config,
            opensearch_client,
            self.logger,
        )
        self.aws_sqs = AwsSQS(
            queue_url=self.config.doc2vec_queue_url,
            aws_region=self.config.aws_region,
            aws_endpoint_url=self.config.aws_endpoint_url or None,
            max_number_of_messages=1,
            wait_time=self.config.sqs_wait_seconds,
            logger=self.logger,
            visibility_timeout=self.config.sqs_visibility_seconds,
            retry_wait_seconds=self.config.retry_seconds,
        )
        self.historical_handler = HistoricalHandler(
            self.config,
            self.session_factory,
            self.opensearch,
            self.aws_sqs,
            self.logger,
        )

    def close(self) -> None:
        """Close the database connection pool when the process stops."""

        self.database.engine.dispose()

    def _create_database_connection(self) -> DatabaseConnection:
        """Create the database connection from the Vault-backed configuration."""

        return DatabaseConnection(
            pg_db_user=self.config.pg_db_user,
            pg_db_password=self.config.pg_db_password,
            pg_db_host=self.config.pg_db_host,
            pg_db_database=self.config.pg_db_database,
            pg_db_port=self.config.pg_db_port,
            pool_size=self.config.database_pool_size,
            max_overflow=self.config.database_max_overflow,
            dynamic_credentials_file=self.config.dynamic_credential_file or None,
            application_name="doc2vec_historical",
            logger=self.logger,
        )
