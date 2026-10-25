from sqlalchemy.orm import sessionmaker

from doc_delete_completed.opensearch import constants
from doc_delete_completed.opensearch.config import Config
from doc_delete_completed.opensearch.db_store.embeddings_db_store import EmbeddingsDBStore
from doc_delete_completed.opensearch.db_store.sentence_vector_db_store import SentenceVectorDBStore
from doc_delete_completed.opensearch.db_store.titan_matches_db_store import TitanMatchesDBStore
from doc_delete_completed.opensearch.handlers.document_delete_handler import DocumentDeleteHandler
from doc_delete_completed.opensearch.opensearch_store.sentence_catalog_open_search_store import (
    SentenceCatalogOpenSearchStore,
)
from doc_delete_completed.opensearch.opensearch_store.sentence_occurrence_open_search_store import (
    SentenceOccurrenceOpenSearchStore,
)
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.aws.opensearch.OpenSearchClient import OpenSearchClient
from ta_utils.aws.sqs.AwsSQS import AwsSQS
from ta_utils.logger.CustomLogger import CustomLogger


class Dependencies:
    """Build and own the database, OpenSearch, SQS, and handler objects."""

    def __init__(
        self,
        custom_logger: CustomLogger | None = None,
        database_connection: DatabaseConnection | None = None,
        config: Config | None = None,
    ) -> None:
        self.logger = custom_logger or CustomLogger("doc_delete")
        self.config = config or Config.from_environment()
        self.database = database_connection or self._create_database_connection()
        self.session_factory = sessionmaker(bind=self.database.engine, autoflush=False, expire_on_commit=False)
        self.opensearch_client = OpenSearchClient(
            opensearch_host=self.config.opensearch_host,
            opensearch_iam_access_role=self.config.opensearch_iam_access_role,
            aws_region=self.config.aws_region,
            opensearch_port=constants.OPENSEARCH_PORT,
            opensearch_service=constants.OPENSEARCH_SERVICE,
        ).create_client()
        self.aws_sqs = AwsSQS(
            queue_url=self.config.receive_sqs_endpoint,
            aws_region=self.config.aws_region,
            aws_endpoint_url=self.config.aws_local_endpoint_url or None,
            max_number_of_messages=1,
            wait_time=constants.SQS_WAIT_SECONDS,
            logger=self.logger,
            visibility_timeout=constants.SQS_VISIBILITY_SECONDS,
        )
        sentence_occurrence_open_search_store = SentenceOccurrenceOpenSearchStore(self.opensearch_client)
        sentence_catalog_open_search_store = SentenceCatalogOpenSearchStore(self.opensearch_client)
        sentence_vector_db_store = SentenceVectorDBStore(self.session_factory)
        embeddings_db_store = EmbeddingsDBStore(self.session_factory)
        titan_matches_db_store = TitanMatchesDBStore(self.session_factory)
        self.document_delete_handler = DocumentDeleteHandler(
            sentence_occurrence_open_search_store,
            sentence_catalog_open_search_store,
            sentence_vector_db_store,
            embeddings_db_store,
            titan_matches_db_store,
            self.aws_sqs,
            self.config.sent_sqs_endpoint,
            self.logger,
            self.config.database_delete_enabled,
        )

    def close(self) -> None:
        """Close the PostgreSQL connection pool when the process stops."""

        self.database.engine.dispose()

    def _create_database_connection(self) -> DatabaseConnection:
        """Create the shared database connection from validated configuration."""

        # Local development uses the credentials already loaded from the YAML
        # file. Other environments let the shared library refresh credentials.
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
            application_name="doc_delete",
            **credential_arguments,
        )
