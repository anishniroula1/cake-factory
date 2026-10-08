from datetime import datetime
from typing import ClassVar

from sqlalchemy import (
    ARRAY,
    Boolean,
    CheckConstraint,
    Column,
    Computed,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    PickleType,
    Sequence,
    SmallInteger,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class TextSimilarityInput(Base):
    """Provide document values shared by every sentence for one tsp_id."""

    __tablename__ = "text_similarity_input"
    __table_args__: ClassVar[dict[str, object]] = {"schema": "tsp"}

    tsp_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    application_id: Mapped[int] = mapped_column(Integer, nullable=False)
    section_name: Mapped[str] = mapped_column(String(50), nullable=False)
    analysis_group: Mapped[str] = mapped_column(String(50), nullable=False)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    document_state: Mapped[str | None] = mapped_column(String(50))


class SentencesVector(Base):
    """Represent one sentence occurrence saved by Doc2Vec."""

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
    section_name: Mapped[str] = mapped_column(String(50), nullable=False)
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
    closest_nm_score: Mapped[float | None] = mapped_column(Float)
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
    sentence_key: Mapped[str] = mapped_column(Text, nullable=False)

    tsp: Mapped[TextSimilarityInput] = relationship()


class TitanMatches(Base):
    """Store one of the 16 match buckets owned by a sentence occurrence."""

    __tablename__ = "titan_matches"
    __table_args__ = (
        CheckConstraint(
            "match_bucket_number BETWEEN 0 AND 15",
            name="ck_titan_matches_bucket_number",
        ),
        CheckConstraint(
            "jsonb_typeof(matches) = 'object'",
            name="ck_titan_matches_json_object",
        ),
        Index("ix_titan_matches_tsp_id", "tsp_id", "global_id"),
        Index(
            "ix_titan_matches_application_id",
            "application_id",
            "global_id",
        ),
        {"extend_existing": True, "schema": "tsp"},
    )

    global_id = Column(
        Integer,
        ForeignKey("tsp.sentences_vector.global_id"),
        primary_key=True,
    )
    match_bucket_number = Column(
        SmallInteger,
        primary_key=True,
    )
    tsp_id = Column(
        Integer,
        ForeignKey("tsp.text_similarity_input.tsp_id"),
        nullable=False,
    )
    application_id = Column(Integer, nullable=False)
    matches = Column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::JSONB"),
    )
    match_count = Column(
        Integer,
        Computed("tsp.titan_match_object_size(matches)", persisted=True),
        nullable=False,
    )
    verbatim = Column(
        ARRAY(Integer),
        Computed("tsp.titan_verbatim_global_ids(matches)", persisted=True),
        nullable=False,
    )
    last_modified = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    tsp = relationship("TextSimilarityInput")
    sen_vec = relationship("SentencesVector")
