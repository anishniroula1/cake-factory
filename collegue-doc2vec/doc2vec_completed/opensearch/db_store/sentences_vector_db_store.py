from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import bindparam, func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, joinedload, load_only

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.batching import batches
from doc2vec_completed.opensearch.contracts import Classification, TspId
from doc2vec_completed.opensearch.text import sentence_key, sentence_word_count
from ta_db_connection.atatables import SentencesVector, TextSimilarityInput


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

    def load_sentences_batch_by_tsp_id(
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

    @staticmethod
    def prepare_sentence_metadata(
        sentence_vector_records: list[SentencesVector],
    ) -> tuple[int, int]:
        """Prepare sentence keys and word counts without writing PostgreSQL.

        Input: 30 SentencesVector records where 10 already contain sentence_key.
        Output: (20, 10), meaning 20 keys were generated and 10 were reused.

        The prepared values stay on the records while OpenSearch creates or
        reuses their vectors and tracer classifications. One later database
        call saves all prepared fields together.
        """

        current_time = datetime.now(UTC)
        generated_count = 0
        reused_count = 0
        for sentence_vector_record in sentence_vector_records:
            if sentence_vector_record.sentence_key:
                reused_count += 1
            else:
                sentence_vector_record.sentence_key = sentence_key(
                    sentence_vector_record.sentence_content
                )
                generated_count += 1
            sentence_vector_record.word_count = sentence_word_count(
                sentence_vector_record.sentence_content
            )
            sentence_vector_record.last_modified = current_time
        return generated_count, reused_count

    def save_sentence_metadata_and_classifications(
        self,
        sentence_vector_records: list[SentencesVector],
        classifications: dict[str, Classification],
    ) -> int:
        """Save all prepared sentence fields with one database update.

        Input: two records sharing key A and classification data for key A.
        Output: 2 after both rows receive their key, word count, and flags.

        This is the only sentences_vector update used by historical records
        and live records that were already stored by an earlier attempt.
        """

        current_time = datetime.now(UTC)
        updates = []
        for sentence_vector_record in sentence_vector_records:
            classification = classifications[sentence_vector_record.sentence_key]
            sentence_vector_record.is_non_matcher = classification["is_non_matcher"]
            sentence_vector_record.is_tracer = classification["is_tracer"]
            sentence_vector_record.is_form_language = classification["is_form_language"]
            sentence_vector_record.last_modified = current_time
            updates.append(
                {
                    "record_global_id": sentence_vector_record.global_id,
                    "record_sentence_key": sentence_vector_record.sentence_key,
                    "record_word_count": sentence_vector_record.word_count,
                    "record_is_non_matcher": classification["is_non_matcher"],
                    "record_is_tracer": classification["is_tracer"],
                    "record_is_form_language": classification["is_form_language"],
                    "record_last_modified": current_time,
                }
            )

        if not updates:
            return 0
        table = SentencesVector.__table__
        statement = (
            update(table)
            .where(table.c.global_id == bindparam("record_global_id"))
            .values(
                sentence_key=bindparam("record_sentence_key"),
                word_count=bindparam("record_word_count"),
                is_non_matcher=bindparam("record_is_non_matcher"),
                is_tracer=bindparam("record_is_tracer"),
                is_form_language=bindparam("record_is_form_language"),
                last_modified=bindparam("record_last_modified"),
            )
        )
        with self.session_factory() as session:
            try:
                session.execute(statement, updates)
                session.commit()
            except SQLAlchemyError:
                session.rollback()
                raise
        return len(updates)

    @staticmethod
    def build_unsaved_document_sentence_records(
        document: TextSimilarityInput,
        sentences: list[tuple[int, str]],
    ) -> list[SentencesVector]:
        """Build unsaved live sentence records for catalog classification.

        Input: document tsp_id=10 and two numbered sentence strings.
        Output: two in-memory records containing keys and word counts but no
        database-generated global IDs.
        """

        current_time = datetime.now(UTC)
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
            for local_id, content in sentences
        ]
        for sentence_vector_record in sentence_vector_records:
            sentence_vector_record.tsp = document
        return sentence_vector_records

    def insert_classified_document_sentences(
        self,
        document: TextSimilarityInput,
        sentences: list[tuple[int, str]],
        classifications: dict[str, Classification],
    ) -> list[SentencesVector]:
        """Insert fully prepared live sentences in one transaction.

        Input: document={tsp_id=1,content="this is a real sentence. sentences exist",...}
        and sentences=[(1, "this is a real sentence.")], plus classifications
        mapped by sentence key.

        Output: saved SentencesVector records containing generated global IDs,
        sentence keys, word counts, and tracer classification flags.
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
                    sentence_vector_records = []
                    for local_id, content in sentence_batch:
                        record_sentence_key = sentence_key(content)
                        classification = classifications[record_sentence_key]
                        sentence_vector_records.append(
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
                                is_non_matcher=classification["is_non_matcher"],
                                is_tracer=classification["is_tracer"],
                                is_form_language=classification[
                                    "is_form_language"
                                ],
                                last_modified=current_time,
                                sentence_key=record_sentence_key,
                            )
                        )
                    session.add_all(sentence_vector_records)

                    # Flush assigns global IDs, but these records are still not
                    # permanent. A later failure can roll back every batch.
                    session.flush()
                    saved_sentence_vector_records.extend(sentence_vector_records)

                session.commit()
            except SQLAlchemyError:
                session.rollback()
                raise

            # OpenSearch needs the parent document fields. Assign the loaded
            # record so worker threads do not make lazy database queries.
            for sentence_vector_record in saved_sentence_vector_records:
                sentence_vector_record.tsp = document
            return saved_sentence_vector_records

    def count_training_sentences_by_tsp_id(self, tsp_id: TspId) -> int:
        """Count classification-eligible sentences considered for IVF training.

        Input: tsp_id=1040
        Output: 950 when 950 records pass the three classification flags.

        The handler applies the meaningful-word rule after loading each page
        because word_count stored on older rows is not reliable.
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
        """Load the next classification-eligible model-training sentence page.

        Input: tsp_id=1040, after_global_id=0, page_size=1000
        Output: up to 1000 records ordered by global_id. The handler calculates
        their current meaningful-word count before building training documents.
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

