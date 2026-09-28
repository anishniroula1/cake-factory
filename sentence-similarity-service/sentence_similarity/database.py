from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sentence_similarity.config import Config


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

    def close(self):
        """Release every pooled database connection."""

        self.engine.dispose()
