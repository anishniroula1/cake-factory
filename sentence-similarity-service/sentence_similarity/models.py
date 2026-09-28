from datetime import datetime
from typing import ClassVar

from sqlalchemy import BigInteger, Boolean, DateTime, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TextSimilarityInput(Base):
    __tablename__ = "text_similarity_input"
    __table_args__: ClassVar = {"schema": "tsp"}

    tsp_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    document_state: Mapped[str] = mapped_column(Text)


class SentenceVector(Base):
    __tablename__ = "sentences_vector"

    global_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    application_id: Mapped[str] = mapped_column(Text)
    tsp_id: Mapped[int] = mapped_column(BigInteger)
    analysis_group: Mapped[str] = mapped_column(Text)
    sentence_key: Mapped[str] = mapped_column(Text)
    is_current: Mapped[bool] = mapped_column(Boolean)
    is_non_matcher: Mapped[bool] = mapped_column(Boolean)
    is_tracer: Mapped[bool] = mapped_column(Boolean)
    is_form_language: Mapped[bool] = mapped_column(Boolean)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
