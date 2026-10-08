from sqlalchemy import URL, create_engine


class DatabaseConnection:
    def __init__(
        self,
        pg_db_user: str,
        pg_db_password: str,
        pg_db_host: str,
        pg_db_database: str,
        pg_db_port: int = 5432,
        pool_size: int = 30,
        max_overflow: int = 10,
        dynamic_credentials_file: str | None = None,
        application_name: str = "sentence_similarity",
    ) -> None:
        """Create the PostgreSQL connection pool used by sentence workers."""

        self.dynamic_credentials_file = dynamic_credentials_file
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
            pool_recycle=1_800,
            max_overflow=max_overflow,
            future=True,
            connect_args={"application_name": application_name},
        )
