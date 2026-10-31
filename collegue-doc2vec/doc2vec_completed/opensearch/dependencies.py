from sqlalchemy.orm import sessionmaker

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.config import Config
from doc2vec_completed.opensearch.handlers.historical_handler import HistoricalHandler
from doc2vec_completed.opensearch.handlers.live_handler import LiveHandler
from doc2vec_completed.opensearch.handlers.tracer_handler import TracerHandler
from doc2vec_completed.opensearch.handlers.train_model_handler import TrainModelHandler
from doc2vec_completed.opensearch.opensearch_helper import OpenSearchHelper
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.aws.opnesearch.open_search_client import OpenSearchClient
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

        opensearch_client = OpenSearchClient(
            opensearch_host=self.config.opensearch_host,
            opensearch_iam_access_role=self.config.opensearch_iam_access_role,
            aws_region=self.config.aws_region,
            opensearch_port=constants.OPENSEARCH_PORT,
            opensearch_service=constants.OPENSEARCH_SERVICE,
            pool_maxsize=20,
            role_session_name="doc2vecSentenceIngestion",
        ).create_client()
        self.opensearch_helper = OpenSearchHelper(
            opensearch_client,
            self.logger,
        )
        self.aws_sqs = AwsSQS(
            queue_url=self.config.doc2vec_queue_url,
            aws_region=self.config.aws_region,
            aws_endpoint_url=self.config.aws_local_endpoint_url or None,
            max_number_of_messages=1,
            wait_time=constants.SQS_WAIT_SECONDS,
            logger=self.logger,
            visibility_timeout=constants.SQS_VISIBILITY_SECONDS,
            retry_wait_seconds=constants.RETRY_SECONDS,
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
            self.session_factory,
            self.opensearch_helper,
            self.logger,
        )
        self.train_model_handler = TrainModelHandler(
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
            pool_size=constants.DATABASE_POOL_SIZE,
            max_overflow=constants.DATABASE_MAX_OVERFLOW,
            application_name="doc2vec_opensearch",
            **credential_arguments,
        )

