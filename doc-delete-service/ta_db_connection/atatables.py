from typing import ClassVar

from sqlalchemy import ARRAY, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TextSimilarityInput(Base):
    """Map the document fields required while deleting sentence-owned data."""

    __tablename__ = "text_similarity_input"
    __table_args__: ClassVar[dict[str, object]] = {"schema": "tsp"}

    tsp_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    application_id: Mapped[int] = mapped_column(Integer, nullable=False)
    analysis_group: Mapped[str] = mapped_column(String(50), nullable=False)


class SentencesVector(Base):
    """Map each sentence occurrence owned by one tsp_id."""

    __tablename__ = "sentences_vector"
    __table_args__: ClassVar[dict[str, object]] = {"extend_existing": True, "schema": "tsp"}

    global_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tsp_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("tsp.text_similarity_input.tsp_id"), nullable=False
    )
    analysis_group: Mapped[str] = mapped_column(String(50), nullable=False)
    sentence_key: Mapped[str | None] = mapped_column(Text)


class Embeddings(Base):
    """Map saved sentence vectors removed with their source global IDs."""

    __tablename__ = "embeddings"
    __table_args__: ClassVar[dict[str, object]] = {"extend_existing": True, "schema": "tsp"}

    global_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    embedding_type: Mapped[int] = mapped_column(Integer, primary_key=True)
    embeddings: Mapped[list[float]] = mapped_column(ARRAY(Float))

