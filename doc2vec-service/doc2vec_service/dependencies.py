from sqlalchemy.orm import sessionmaker

from doc2vec_service.AwsSQS import AwsSQS
from doc2vec_service.config import Config
from doc2vec_service.contracts import DatabaseProvider, Logger
from doc2vec_service.custom_logger import CustomLogger
from doc2vec_service.DatabaseConnection import DatabaseConnection
from doc2vec_service.handlers.historical_handler import HistoricalHandler
from doc2vec_service.open_search_client import OpenSearchClient
from doc2vec_service.opensearch import OpenSearchHelper


class Dependencies:
    def __init__(
        self,
        customLogger: Logger | None = None,
        databaseConnection: DatabaseProvider | None = None,
    ) -> None:
        """Create everything used while processing an SQS message."""

        self.logger = customLogger or CustomLogger("doc2vec_service")
        self.config = Config.fromEnvironment()
        self.database = databaseConnection or self._createDatabaseConnection()
        self.sessionFactory = sessionmaker(
            bind=self.database.engine,
            autoflush=False,
            expire_on_commit=False,
        )

        opensearchClient = OpenSearchClient(self.config).create_client()
        self.opensearch = OpenSearchHelper(
            self.config,
            opensearchClient,
            self.logger,
        )
        self.awsSqs = AwsSQS(
            queue_url=self.config.sourceQueueUrl,
            aws_region=self.config.awsRegion,
            max_number_of_messages=1,
            wait_time=self.config.sqsWaitSeconds,
            logger=self.logger,
            visibility_timeout=self.config.sqsVisibilitySeconds,
            retry_wait_seconds=self.config.retrySeconds,
        )
        self.historicalHandler = HistoricalHandler(
            self.config,
            self.sessionFactory,
            self.opensearch,
            self.awsSqs,
            self.logger,
        )

    def close(self) -> None:
        """Close the database connection pool when the process stops."""

        self.database.engine.dispose()

    def _createDatabaseConnection(self) -> DatabaseConnection:
        """Create the local connection from the configured database URL."""

        return DatabaseConnection(
            pg_db_user=self.config.pgDbUser,
            pg_db_password=self.config.pgDbPassword,
            pg_db_host=self.config.pgDbHost,
            pg_db_database=self.config.pgDbDatabase,
            pg_db_port=self.config.pgDbPort,
            pool_size=self.config.databasePoolSize,
            max_overflow=self.config.databaseMaxOverflow,
            application_name="doc2vec_historical",
            logger=self.logger,
        )
