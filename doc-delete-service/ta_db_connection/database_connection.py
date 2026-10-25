from sqlalchemy import URL, create_engine


class DatabaseConnection:
    """Create and expose the SQLAlchemy PostgreSQL connection pool."""

    def __init__(
        self,
        pg_db_user: str,
        pg_db_password: str,
        pg_db_host: str,
        pg_db_database: str,
        pg_db_port: int = 5432,
        pool_size: int = 20,
        pool_recycle_time: int = 1800,
        max_overflow: int = 10,
        use_dynamic_credentials: bool = False,
        dynamic_credentials_file: str | None = None,
        application_name: str = "doc_delete",
    ) -> None:
        self.use_dynamic_credentials = use_dynamic_credentials
        self.dynamic_credentials_file = dynamic_credentials_file
        if use_dynamic_credentials:
            raise ValueError("Pass the shared DatabaseConnection instance when dynamic credentials are enabled")

        connection_string = URL.create(
            "postgresql+psycopg",
            username=pg_db_user,
            password=pg_db_password,
            host=pg_db_host,
            port=pg_db_port,
            database=pg_db_database,
        )
        self.engine = create_engine(
            connection_string,
            pool_size=pool_size,
            pool_pre_ping=True,
            pool_recycle=pool_recycle_time,
            max_overflow=max_overflow,
            future=True,
            connect_args={"application_name": application_name},
        )

