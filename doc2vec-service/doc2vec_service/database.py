from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from doc2vec_service.config import Config


class Database:
    def __init__(self, config: Config):
        """Create a thread-safe SQLAlchemy session factory."""

        self.engine = create_engine(
            config.databaseUrl,
            pool_pre_ping=True,
            pool_size=config.databasePoolSize,
            max_overflow=config.databaseMaxOverflow,
        )
        self.sessionFactory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            expire_on_commit=False,
        )

    @contextmanager
    def session(self) -> Iterator[Session]:
        """Yield one session and always close it after use."""

        with self.sessionFactory() as session:
            yield session

    def close(self):
        """Release database connections during application shutdown."""

        self.engine.dispose()
