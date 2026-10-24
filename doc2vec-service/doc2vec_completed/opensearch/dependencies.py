from sqlalchemy.orm import sessionmaker

from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.handlers.historical_handler import HistoricalHandler
from doc2vec_completed.opensearch.handlers.live_handler import LiveHandler
from doc2vec_completed.opensearch.handlers.tracer_handler import TracerHandler
from doc2vec_completed.opensearch.handlers.train_model_handler import TrainModelHandler
from doc2vec_completed.opensearch.open_search_client import OpenSearchClient
from doc2vec_completed.opensearch.open_search_helper import OpenSearchHelper
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class Dependencies:
    """Build and own the shared resources used by the Doc2Vec worker."""

    def __init__(
        self,
        custom_logger: CustomLogger | None = None,
        database_connection: DatabaseConnection | None = None,
    ) -> None:
        self.logger = custom_logger or CustomLogger("doc2vec_completed.opensearch")
        self.config = Config.from_environment()
        self.database = database_connection or self._create_database_connection()
        self.session_factory = sessionmaker(
            bind=self.database.engine,
            autoflush=False,
            expire_on_commit=False,
        )

        opensearch_client = OpenSearchClient(self.config).create_client()
        self.opensearch_helper = OpenSearchHelper(
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
        self.live_handler = LiveHandler(
            self.config,
            self.session_factory,
            self.opensearch_helper,
            self.aws_sqs,
            self.logger,
        )
        self.historical_handler = HistoricalHandler(
            self.config,
            self.session_factory,
            self.opensearch_helper,
            self.aws_sqs,
            self.logger,
        )
        self.tracer_handler = TracerHandler(
            self.config,
            self.session_factory,
            self.opensearch_helper,
            self.logger,
        )
        self.train_model_handler = TrainModelHandler(
            self.config,
            self.session_factory,
            self.opensearch_helper,
            self.logger,
        )

    def close(self) -> None:
        """Close the database pool when the process stops."""

        self.database.engine.dispose()

    def _create_database_connection(self) -> DatabaseConnection:
        """Create the database connection from Vault-backed configuration."""

        credential_arguments = (
            {"use_dynamic_credentials": False}
            if self.config.environment == "local"
            else {
                "use_dynamic_credentials": True,
                "dynamic_credentials_file": self.config.dynamic_credential_file,
            }
        )
        return DatabaseConnection(
            pg_db_user=self.config.pg_db_user,
            pg_db_password=self.config.pg_db_password,
            pg_db_host=self.config.pg_db_host,
            pg_db_database=self.config.pg_db_database,
            pg_db_port=self.config.pg_db_port,
            pool_size=self.config.database_pool_size,
            max_overflow=self.config.database_max_overflow,
            application_name="doc2vec_opensearch",
            **credential_arguments,
        )
