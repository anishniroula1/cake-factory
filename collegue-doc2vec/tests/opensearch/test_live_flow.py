from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import SQLAlchemyError

from doc2vec_completed.opensearch import constants
from doc2vec_completed.opensearch.db_store import SentenceVectorDBStore
from doc2vec_completed.opensearch.handlers.live_handler import LiveHandler
from doc2vec_completed.opensearch.sentence_splitter import split_document
from doc2vec_completed.opensearch.sqs_processor import SqsProcessor
from ta_db_connection.atatables import SentencesVector, TextSimilarityInput
from tests.opensearch.fakes import OpenSearchFake, SessionFake, SessionSequence, SqsFake


class TextSimilarityInputStoreFake:
    def __init__(self, document):
        self.document = document
        self.calls = []

    def retrieve_document_from_tsp_id(self, tsp_id):
        self.calls.append(tsp_id)
        return self.document


class LiveSentenceStoreFake:
    def __init__(self, existing_rows=None):
        self.calls = []
        self.next_global_id = 1
        self.existing_rows = existing_rows or []

    def load_sentences_batch_by_tsp_id(self, tsp_id, after_global_id, page_size):
        return [
            row
            for row in self.existing_rows
            if row.global_id > after_global_id
        ][:page_size]

    def save_missing_sentence_keys(self, sentence_vector_records):
        generated_key_count = 0
        reused_key_count = 0
        for sentence_vector_record in sentence_vector_records:
            if sentence_vector_record.sentence_key:
                reused_key_count += 1
                continue
            sentence_vector_record.sentence_key = (
                f"{sentence_vector_record.global_id:064x}"
            )
            generated_key_count += 1
        return generated_key_count, reused_key_count

    def save_document_sentences(self, document, sentences):
        self.calls.append(sentences)
        rows = []
        for local_id, content in sentences:
            row = SentencesVector(
                global_id=self.next_global_id,
                tsp_id=document.tsp_id,
                a_number=document.a_number,
                section_name=document.section_name,
                sent_id_local=local_id,
                sentence_content=content,
                word_count=4,
                source_type=document.source_type,
                analysis_group=document.analysis_group,
                is_current=document.is_current,
                embedding_list=[],
                is_non_matcher=False,
                is_form_language=False,
                is_tracer=False,
                in_scope=True,
                sentence_key=str(self.next_global_id) * 64,
                last_modified=datetime.now(UTC),
                tsp=document,
            )
            rows.append(row)
            self.next_global_id += 1
        return rows


class HandlerFake:
    def __init__(self):
        self.calls = []

    def process(self, value):
        self.calls.append(value)
        return {"indexed_count": 1}


def live_document(content="First sentence. Second sentence."):
    return TextSimilarityInput(
        tsp_id=10,
        a_number=123456789,
        application_id=1001,
        content=content,
        source_type="AFFIDAVIT",
        section_name="affidavit",
        analysis_group="Asylee",
        is_current=True,
    )


def build_handler(service_config, custom_logger, document):
    opensearch = OpenSearchFake()
    sqs = SqsFake()
    handler = LiveHandler(
        service_config,
        SessionSequence([]),
        opensearch,
        sqs,
        custom_logger,
    )
    handler.text_similarity_input_db_store = TextSimilarityInputStoreFake(document)
    handler.sentence_vector_db_store = LiveSentenceStoreFake()
    return handler, opensearch, sqs


def test_live_handler_saves_indexes_refreshes_and_sends_sqs_message(
    service_config,
    custom_logger,
):
    sentences = ["First sentence.", "Second sentence."]
    handler, opensearch, sqs = build_handler(
        service_config,
        custom_logger,
        live_document("First sentence.\nÊSecond sentence."),
    )

    result = handler.process(10)

    assert result["indexed_count"] == 2
    assert handler.sentence_vector_db_store.calls == [
        [(1, sentences[0]), (2, sentences[1])]
    ]
    assert [needs_embedding for _, _, needs_embedding in opensearch.bulk_calls] == [
        True,
        False,
    ]
    assert opensearch.refresh_alias_calls == [["catalog-asylee", "occurrence"]]
    assert sqs.sent[0]["message_body"] == {
        "source": "P2",
        "tsp_id": 10,
        "sent_by": "doc2vec",
        "success": True,
        "reason": "",
    }


def test_live_handler_reports_no_content_without_indexing(
    service_config,
    custom_logger,
):
    handler, opensearch, sqs = build_handler(
        service_config,
        custom_logger,
        live_document(" ¶  "),
    )

    result = handler.process(10)

    assert result["reason"] == "no_content"
    assert opensearch.refresh_alias_calls == []
    assert sqs.sent[0]["message_body"]["success"] is False


def test_live_handler_filters_refugee_rrf_sections_before_splitting(
    service_config,
    custom_logger,
):
    document = live_document(
        "1. BIOGRAPHY Remove this text. "
        "4. REFUGEE CLAIM Keep the refugee claim. "
        "5. NEED FOR RESETTLEMENT Remove this section. "
        "6. SPECIFIC NEEDS Keep the specific needs."
    )
    document.analysis_group = "Refugee"
    document.section_name = "RRF"
    handler, _, _ = build_handler(service_config, custom_logger, document)

    handler.process(10)

    saved_content = " ".join(
        sentence for _, sentence in handler.sentence_vector_db_store.calls[0]
    )
    assert "Remove this text" not in saved_content
    assert "Remove this section" not in saved_content
    assert "Keep the refugee claim" in saved_content
    assert "Keep the specific needs" in saved_content


def test_live_handler_reuses_saved_keyed_rows(
    service_config,
    custom_logger,
    monkeypatch,
):
    document = live_document()
    existing_rows = [
        SentencesVector(
            global_id=global_id,
            tsp_id=10,
            sent_id_local=local_id,
            sentence_content=f"Already saved sentence {local_id}.",
            source_type="affidavit",
            sentence_key=key_character * 64,
            last_modified=datetime.now(UTC),
            tsp=document,
        )
        for global_id, local_id, key_character in [(20, 1, "a"), (21, 2, "b")]
    ]
    handler, opensearch, sqs = build_handler(
        service_config,
        custom_logger,
        document,
    )
    handler.sentence_vector_db_store = LiveSentenceStoreFake(existing_rows)
    monkeypatch.setattr(constants, "DOCUMENT_SENTENCE_PAGE_SIZE", 1)

    result = handler.process(10)

    assert result["indexed_count"] == 2
    assert handler.text_similarity_input_db_store.calls == []
    assert handler.sentence_vector_db_store.calls == []
    assert opensearch.refresh_alias_calls == [["catalog-asylee", "occurrence"]]
    assert sqs.sent[0]["message_body"]["success"] is True


def test_live_handler_fills_missing_keys_without_splitting_again(
    service_config,
    custom_logger,
):
    document = live_document()
    existing_record = SentencesVector(
        global_id=20,
        tsp_id=10,
        sent_id_local=1,
        sentence_content="This sentence was already saved in PostgreSQL.",
        source_type="affidavit",
        sentence_key=None,
        last_modified=datetime.now(UTC),
        tsp=document,
    )
    handler, opensearch, _ = build_handler(
        service_config,
        custom_logger,
        document,
    )
    handler.sentence_vector_db_store = LiveSentenceStoreFake([existing_record])

    result = handler.process(10)

    assert result["indexed_count"] == 1
    assert handler.text_similarity_input_db_store.calls == []
    assert existing_record.sentence_key is not None
    assert opensearch.refresh_alias_calls == [["catalog-asylee", "occurrence"]]


def test_message_without_reason_uses_live_handler(custom_logger):
    source_queue = SqsFake()
    live_handler = HandlerFake()
    dependencies = SimpleNamespace(
        aws_sqs=source_queue,
        logger=custom_logger,
        live_handler=live_handler,
        historical_handler=HandlerFake(),
        train_model_handler=HandlerFake(),
        tracer_handler=HandlerFake(),
    )
    message = {"tsp_id": 10, "ReceiptHandle": "live-receipt"}

    SqsProcessor(dependencies)._process_message(message)

    assert live_handler.calls == [10]
    assert source_queue.deleted == [message]


def test_live_sentence_store_flushes_batches_and_commits_once(monkeypatch):
    document = live_document()
    session = SessionFake()
    store = SentenceVectorDBStore(SessionSequence([session]))
    monkeypatch.setattr(constants, "LIVE_DATABASE_BATCH_SIZE", 1)

    rows = store.save_document_sentences(
        document,
        [(1, "First sentence."), (2, "Second sentence.")],
    )

    assert [row.sent_id_local for row in rows] == [1, 2]
    assert [row.global_id for row in rows] == [1, 2]
    assert rows[1].source_type == "affidavit"
    assert all(len(row.sentence_key) == 64 for row in rows)
    assert session.flushes == 2
    assert session.commits == 1
    assert session.rollbacks == 0


def test_live_sentence_store_rolls_back_every_batch_when_one_flush_fails(monkeypatch):
    session = SessionFake(fail_flush_number=2)
    store = SentenceVectorDBStore(SessionSequence([session]))
    monkeypatch.setattr(constants, "LIVE_DATABASE_BATCH_SIZE", 1)

    with pytest.raises(SQLAlchemyError, match="database flush failed"):
        store.save_document_sentences(
            live_document(),
            [(1, "First sentence."), (2, "Second sentence.")],
        )

    assert session.flushes == 2
    assert session.commits == 0
    assert session.rollbacks == 1


def test_sentence_key_save_restores_records_when_the_database_write_fails(
    sentence_record,
):
    sentence_record.sentence_key = None
    original_last_modified = sentence_record.last_modified
    session = SessionFake(fail_execute=True)
    store = SentenceVectorDBStore(SessionSequence([session]))

    with pytest.raises(SQLAlchemyError, match="database execute failed"):
        store.save_missing_sentence_keys([sentence_record])

    assert sentence_record.sentence_key is None
    assert sentence_record.last_modified == original_last_modified
    assert session.commits == 0
    assert session.rollbacks == 1


def test_syntok_splitter_returns_original_text_and_handles_empty_content():
    content = "I arrived yesterday. Then I stayed with my aunt."

    assert split_document(content) == [
        "I arrived yesterday.",
        "Then I stayed with my aunt.",
    ]
    assert split_document(" ¶ \n") == []

