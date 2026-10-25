from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import bindparam, func, select, update
from sqlalchemy.orm import Session, joinedload, load_only

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.contracts import TspId
from doc2vec_completed.opensearch.text import sentence_key, sentence_word_count
from ta_db_connection.atables import SentencesVector, TextSimilarityInput


class SentenceVectorDBStore:
    """Read sentence pages and save live or generated sentence data."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def count_sentences_by_tsp_id(self, tsp_id: TspId) -> int:
        """Count all SentencesVector records for one document.

        Input: tsp_id=1040
        Output: 65 when the document contains 65 sentence records.
        """

        with self.session_factory() as session:
            statement = (
                select(func.count())
                .select_from(SentencesVector)
                .where(SentencesVector.tsp_id == tsp_id)
            )
            return int(session.scalar(statement) or 0)

    def load_document_sentence_page(
        self,
        tsp_id: TspId,
        after_global_id: int,
        page_size: int,
    ) -> list[SentencesVector]:
        """Load one ordered sentence page with its parent document fields.

        Input: tsp_id=1040, after_global_id=500, page_size=500
        Output: up to 500 records whose global_id is greater than 500.

        Incoming and historical processing share this query. The legacy
        embedding_list column is not loaded because neither path needs it.
        """

        with self.session_factory() as session:
            statement = (
                select(SentencesVector)
                .options(
                    # Avoid embedding_list because some old records contain NumPy
                    # values that SQLAlchemy cannot safely load here.
                    load_only(
                        SentencesVector.global_id,
                        SentencesVector.tsp_id,
                        SentencesVector.sent_id_local,
                        SentencesVector.sentence_content,
                        SentencesVector.sentence_key,
                        SentencesVector.source_type,
                        SentencesVector.last_modified,
                    ),
                    joinedload(SentencesVector.tsp).load_only(
                        TextSimilarityInput.tsp_id,
                        TextSimilarityInput.application_id,
                        TextSimilarityInput.section_name,
                        TextSimilarityInput.analysis_group,
                        TextSimilarityInput.is_current,
                    ),
                )
                .where(
                    SentencesVector.tsp_id == tsp_id,
                    SentencesVector.global_id > after_global_id,
                )
                .order_by(SentencesVector.global_id)
                .limit(page_size)
            )
            return list(session.scalars(statement))

    def save_missing_sentence_keys(
        self,
        sentence_vector_records: list[SentencesVector],
    ) -> tuple[int, int]:
        """Reuse existing keys and save SHA-256 keys for missing records.

        Input: 30 SentencesVector records where 10 already contain sentence_key.
        Output: (20, 10), meaning 20 generated keys and 10 reused keys.
        """

        current_time = datetime.now(UTC)
        updates = []
        prepared_records = []
        reused_count = 0
        for sentence_vector_record in sentence_vector_records:
            if sentence_vector_record.sentence_key:
                reused_count += 1
                continue

            prepared_records.append(
                (
                    sentence_vector_record,
                    sentence_vector_record.sentence_key,
                    sentence_vector_record.last_modified,
                )
            )
            sentence_vector_record.sentence_key = sentence_key(
                sentence_vector_record.sentence_content
            )
            sentence_vector_record.last_modified = current_time
            updates.append(
                {
                    "record_global_id": sentence_vector_record.global_id,
                    "record_sentence_key": sentence_vector_record.sentence_key,
                    "record_last_modified": current_time,
                }
            )

        if not updates:
            return 0, reused_count

        table = SentencesVector.__table__
        statement = (
            update(table)
            .where(table.c.global_id == bindparam("record_global_id"))
            .values(
                sentence_key=bindparam("record_sentence_key"),
                last_modified=bindparam("record_last_modified"),
            )
        )
        try:
            with self.session_factory() as session:
                session.execute(statement, updates)
                session.commit()
        except Exception:
            # Restore in-memory records before retrying the database write.
            for (
                sentence_vector_record,
                original_key,
                original_last_modified,
            ) in prepared_records:
                sentence_vector_record.sentence_key = original_key
                sentence_vector_record.last_modified = original_last_modified
            raise
        return len(updates), reused_count

    def save_document_sentences(
        self,
        document: TextSimilarityInput,
        sentences: list[tuple[int, str]],
    ) -> list[SentencesVector]:
        """Insert all live sentences with one commit for the whole document.

        Input: document for tsp_id=1040 and sentences=[(1, "First sentence."),
        (2, "Second sentence.")]
        Output: two SentencesVector records with global IDs and sentence keys.

        Large documents are added and flushed in batches so generated global
        IDs are available without holding every pending insert in memory. The
        transaction commits only after every batch succeeds. Any error rolls
        back all sentence records for the document.
        """

        if not sentences:
            return []

        current_time = datetime.now(UTC)
        with self.session_factory() as session:
            saved_sentence_vector_records: list[SentencesVector] = []
            try:
                for sentence_batch in batches(
                    sentences,
                    constants.LIVE_DATABASE_BATCH_SIZE,
                ):
                    sentence_vector_records = [
                        SentencesVector(
                            tsp_id=document.tsp_id,
                            a_number=document.a_number,
                            section_name=document.section_name,
                            sent_id_local=local_id,
                            sentence_content=content,
                            word_count=sentence_word_count(content),
                            source_type=document.source_type.strip().lower(),
                            analysis_group=document.analysis_group,
                            is_current=document.is_current,
                            last_modified=current_time,
                            sentence_key=sentence_key(content),
                        )
                        for local_id, content in sentence_batch
                    ]
                    session.add_all(sentence_vector_records)

                    # Flush assigns global IDs, but these records are still not
                    # permanent. A later failure can roll back every batch.
                    session.flush()
                    saved_sentence_vector_records.extend(sentence_vector_records)

                session.commit()
            except Exception:
                session.rollback()
                raise

            # OpenSearch needs the parent document fields. Assign the loaded
            # record so worker threads do not make lazy database queries.
            for sentence_vector_record in saved_sentence_vector_records:
                sentence_vector_record.tsp = document
            return saved_sentence_vector_records

    def count_training_sentences_by_tsp_id(self, tsp_id: TspId) -> int:
        """Count normal sentences that are eligible for IVF model training.

        Input: tsp_id=1040
        Output: 950 when 950 records are eligible for model training.
        """

        with self.session_factory() as session:
            statement = (
                select(func.count())
                .select_from(SentencesVector)
                .where(
                    SentencesVector.tsp_id == tsp_id,
                    SentencesVector.is_non_matcher.is_(False),
                    SentencesVector.is_tracer.is_(False),
                    SentencesVector.is_form_language.is_(False),
                )
            )
            return int(session.scalar(statement) or 0)

    def load_training_sentence_page(
        self,
        tsp_id: TspId,
        after_global_id: int,
        page_size: int,
    ) -> list[SentencesVector]:
        """Load the next ordered page of normal model-training sentences.

        Input: tsp_id=1040, after_global_id=0, page_size=1000
        Output: up to 1000 eligible records ordered by global_id.
        """

        with self.session_factory() as session:
            statement = (
                select(SentencesVector)
                .options(
                    load_only(
                        SentencesVector.global_id,
                        SentencesVector.sentence_content,
                        SentencesVector.last_modified,
                        SentencesVector.is_non_matcher,
                        SentencesVector.is_tracer,
                        SentencesVector.is_form_language,
                    )
                )
                .where(
                    SentencesVector.tsp_id == tsp_id,
                    SentencesVector.global_id > after_global_id,
                    SentencesVector.is_non_matcher.is_(False),
                    SentencesVector.is_tracer.is_(False),
                    SentencesVector.is_form_language.is_(False),
                )
                .order_by(SentencesVector.global_id)
                .limit(page_size)
            )
            return list(session.scalars(statement))
