from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import bindparam, func, select, update
from sqlalchemy.orm import Session, joinedload, load_only

from doc2vec_completed.opensearch.contracts import TspId
from doc2vec_completed.opensearch.text import sentence_key, sentence_word_count
from ta_db_connection.atables import SentencesVector, TextSimilarityInput


class SentenceVectorDBStore:
    """Read sentence pages and save live or generated sentence data."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def count_sentences_by_tsp_id(self, tsp_id: TspId) -> int:
        """Count all sentence rows for one document.

        Input: tsp_id=1040
        Output: 65 when the document contains 65 sentence rows.
        """

        with self.session_factory() as session:
            statement = (
                select(func.count())
                .select_from(SentencesVector)
                .where(SentencesVector.tsp_id == tsp_id)
            )
            return int(session.scalar(statement) or 0)

    def load_historical_sentence_page(
        self,
        tsp_id: TspId,
        after_global_id: int,
        page_size: int,
    ) -> list[SentencesVector]:
        """Load an ordered historical sentence page with its document fields.

        Input: tsp_id=1040, after_global_id=3000, page_size=3000
        Output: up to 3000 rows whose global_id is greater than 3000.
        """

        with self.session_factory() as session:
            statement = (
                select(SentencesVector)
                .options(
                    # Avoid embedding_list because some old rows contain NumPy
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
        rows: list[SentencesVector],
    ) -> tuple[int, int]:
        """Reuse existing keys and save SHA-256 keys only for missing rows.

        Input: 30 rows where 10 already contain sentence_key.
        Output: (20, 10), meaning 20 generated keys and 10 reused keys.
        """

        current_time = datetime.now(UTC)
        updates = []
        prepared_rows = []
        reused_count = 0
        for row in rows:
            if row.sentence_key:
                reused_count += 1
                continue

            prepared_rows.append((row, row.sentence_key, row.last_modified))
            row.sentence_key = sentence_key(row.sentence_content)
            row.last_modified = current_time
            updates.append(
                {
                    "record_global_id": row.global_id,
                    "record_sentence_key": row.sentence_key,
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
            # Put the in-memory rows back before retrying the whole database write.
            for row, original_key, original_last_modified in prepared_rows:
                row.sentence_key = original_key
                row.last_modified = original_last_modified
            raise
        return len(updates), reused_count

    def save_live_sentence_batch(
        self,
        document: TextSimilarityInput,
        sentences: list[tuple[int, str]],
    ) -> list[SentencesVector]:
        """Insert or update one ordered batch of live document sentences.

        Input: document for tsp_id=1040 and sentences=[(1, "First sentence."),
        (2, "Second sentence.")]
        Output: two SentencesVector rows with global IDs and sentence keys.

        The local sentence number makes a retried message safe. A row already
        stored for the same tsp_id and sent_id_local is updated, while a missing
        row is inserted. Each returned row is ready for OpenSearch indexing.
        """

        if not sentences:
            return []

        local_ids = [local_id for local_id, _ in sentences]
        current_time = datetime.now(UTC)
        with self.session_factory() as session:
            statement = (
                select(SentencesVector)
                .options(
                    # Do not load the legacy embedding_list column. Some old
                    # rows contain NumPy values that MutableList cannot read.
                    load_only(
                        SentencesVector.global_id,
                        SentencesVector.tsp_id,
                        SentencesVector.sent_id_local,
                        SentencesVector.sentence_content,
                        SentencesVector.word_count,
                        SentencesVector.source_type,
                        SentencesVector.analysis_group,
                        SentencesVector.is_current,
                        SentencesVector.sentence_key,
                        SentencesVector.last_modified,
                    )
                )
                .where(
                    SentencesVector.tsp_id == document.tsp_id,
                    SentencesVector.sent_id_local.in_(local_ids),
                )
            )
            existing_rows = {
                row.sent_id_local: row for row in session.scalars(statement)
            }
            saved_rows: list[SentencesVector] = []
            new_rows: list[SentencesVector] = []

            for local_id, content in sentences:
                row = existing_rows.get(local_id)
                if row is None:
                    row = SentencesVector(
                        tsp_id=document.tsp_id,
                        a_number=document.a_number,
                        section_name=document.section_name,
                        sent_id_local=local_id,
                        sentence_content=content,
                        word_count=sentence_word_count(content),
                        source_type=document.source_type.strip().lower(),
                        analysis_group=document.analysis_group,
                        is_current=document.is_current,
                        embedding_list=[],
                        is_non_matcher=False,
                        closest_non_matcher=None,
                        closest_nn_score=None,
                        is_form_language=False,
                        is_tracer=False,
                        last_modified=current_time,
                        in_scope=True,
                        sentence_key=sentence_key(content),
                    )
                    new_rows.append(row)
                else:
                    # A retry may reach a batch already committed before a later
                    # OpenSearch or SQS operation failed. Keep that row and make
                    # its live document fields match the current source text.
                    row.a_number = document.a_number
                    row.section_name = document.section_name
                    row.sentence_content = content
                    row.word_count = sentence_word_count(content)
                    row.source_type = document.source_type.strip().lower()
                    row.analysis_group = document.analysis_group
                    row.is_current = document.is_current
                    row.sentence_key = sentence_key(content)
                    row.last_modified = current_time
                saved_rows.append(row)

            session.add_all(new_rows)
            session.flush()
            session.commit()

            # OpenSearch needs fields from the parent document. Assign the
            # already-loaded row so worker threads do not make lazy DB queries.
            for row in saved_rows:
                row.tsp = document
            return saved_rows

    def count_training_sentences_by_tsp_id(self, tsp_id: TspId) -> int:
        """Count normal sentences that are eligible for IVF model training.

        Input: tsp_id=1040
        Output: 950 when 950 rows are not tracer, form-language, or non-matcher.
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
        Output: up to 1000 eligible rows ordered by global_id.
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
