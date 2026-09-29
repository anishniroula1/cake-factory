from datetime import datetime

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Integer,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SentenceVector(Base):
    __tablename__ = "sentences_vector"

    global_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    application_id: Mapped[str] = mapped_column(Text)
    a_number: Mapped[str] = mapped_column(Text)
    tsp_id: Mapped[int] = mapped_column(BigInteger)
    section_name: Mapped[str] = mapped_column(Text)
    is_non_matcher: Mapped[bool] = mapped_column(Boolean, default=False)
    is_tracer: Mapped[bool] = mapped_column(Boolean, default=False)
    is_form_language: Mapped[bool] = mapped_column(Boolean, default=False)
    sent_id_local: Mapped[int] = mapped_column(BigInteger)
    sentence_content: Mapped[str] = mapped_column(Text)
    source_type: Mapped[str] = mapped_column(Text)
    analysis_group: Mapped[str] = mapped_column(Text)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True)
    sentence_key: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class Embedding(Base):
    __tablename__ = "embeddings"

    global_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    embedding_type: Mapped[int] = mapped_column(Integer, primary_key=True)
    embedding: Mapped[list] = mapped_column(ARRAY(Float))
