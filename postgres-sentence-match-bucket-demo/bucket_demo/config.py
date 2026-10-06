import os
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    postgresHost: str
    postgresPort: int
    postgresDatabase: str
    postgresUser: str
    postgresPassword: str

    @property
    def databaseUrl(self):
        """Build the SQLAlchemy PostgreSQL connection URL."""

        user = quote_plus(self.postgresUser)
        password = quote_plus(self.postgresPassword)

        return (
            f"postgresql+psycopg://{user}:{password}"
            f"@{self.postgresHost}:{self.postgresPort}"
            f"/{self.postgresDatabase}"
        )


@lru_cache
def getSettings():
    """Read the small set of settings used by this demo."""

    return Settings(
        postgresHost=os.getenv("POSTGRES_HOST", "localhost"),
        postgresPort=int(os.getenv("POSTGRES_PORT", "5434")),
        postgresDatabase=os.getenv("POSTGRES_DB", "sentence_matches"),
        postgresUser=os.getenv("POSTGRES_USER", "sentence_user"),
        postgresPassword=os.getenv("POSTGRES_PASSWORD", "sentence_password"),
    )
