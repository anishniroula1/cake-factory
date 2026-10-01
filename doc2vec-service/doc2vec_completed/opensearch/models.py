from datetime import datetime
from typing import ClassVar

from sqlalchemy import (
    ARRAY,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    PickleType,
    Sequence,
    String,
    Text,
    func,
)
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class TextSimilarityInput(Base):
    """Provide the application ID related to a sentence document."""

    __tablename__ = "text_similarity_input"
    __table_args__: ClassVar[dict[str, object]] = {"schema": "tsp"}

    tsp_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    application_id: Mapped[str] = mapped_column(Text, nullable=False)


class SentencesVector(Base):
    __tablename__ = "sentences_vector"
    __table_args__: ClassVar[dict[str, object]] = {
        "extend_existing": True,
        "schema": "tsp",
    }

    global_id: Mapped[int] = mapped_column(
        Integer,
        Sequence("sentences_vector_id_seq", schema="tsp"),
        primary_key=True,
    )
    tsp_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tsp.text_similarity_input.tsp_id"),
        nullable=False,
    )
    a_number: Mapped[int] = mapped_column(Integer, nullable=False)
    section_name: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="p2_document",
    )
    sent_id_local: Mapped[int] = mapped_column(Integer, nullable=False)
    sentence_content: Mapped[str] = mapped_column(Text, nullable=False)
    word_count: Mapped[int] = mapped_column(Integer, nullable=False)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    analysis_group: Mapped[str] = mapped_column(String(50), nullable=False)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    embedding_list: Mapped[list] = mapped_column(
        MutableList.as_mutable(PickleType),
        default=list,
    )
    is_non_matcher: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )
    closest_non_matcher: Mapped[int | None] = mapped_column(Integer)
    closest_nn_score: Mapped[float | None] = mapped_column(Float)
    is_form_language: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )
    is_tracer: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )
    last_modified: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    in_scope: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    sentence_key: Mapped[str | None] = mapped_column(Text)

    tsp: Mapped[TextSimilarityInput] = relationship()


class Embeddings(Base):
    __tablename__ = "embeddings"

    global_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    embedding_type: Mapped[int] = mapped_column(Integer, primary_key=True)
    embeddings: Mapped[list[float]] = mapped_column(ARRAY(Float))
