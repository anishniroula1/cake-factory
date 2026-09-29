from sqlalchemy import URL, create_engine


class DatabaseConnection:
    def __init__(
        self,
        pg_db_user: str,
        pg_db_password: str,
        pg_db_host: str,
        pg_db_database: str,
        pg_db_port: int = 5432,
        pool_size: int = 20,
        pool_recycle_time: int = 1_800,
        max_overflow: int = 10,
        prefect_logger=None,
        use_dynamic_credentials: bool = False,
        dynamic_credentials_file: str | None = None,
        application_name: str = "doc2vec_historical",
        logger=None,
    ):
        """Create the local database connection used by the standalone worker."""

        self.logger = prefect_logger or logger
        self.pg_db_user = pg_db_user
        self.pg_db_password = pg_db_password
        self.pg_db_host = pg_db_host
        self.pg_db_database = pg_db_database
        self.pg_db_port = pg_db_port
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.use_dynamic_credentials = use_dynamic_credentials
        self.dynamic_credentials_file = dynamic_credentials_file
        self.application_name = application_name

        if use_dynamic_credentials:
            raise ValueError(
                "Pass the shared DatabaseConnection instance when dynamic "
                "credentials are enabled"
            )

        self.connection_string = URL.create(
            "postgresql+psycopg",
            username=pg_db_user,
            password=pg_db_password,
            host=pg_db_host,
            port=pg_db_port,
            database=pg_db_database,
        )
        self.engine = create_engine(
            self.connection_string,
            pool_size=pool_size,
            pool_pre_ping=True,
            pool_recycle=pool_recycle_time,
            max_overflow=max_overflow,
            future=True,
            connect_args={"application_name": application_name},
        )

        if self.logger:
            self.logger.info(
                f"Created PostgreSQL pool for application {application_name}"
            )
