from types import SimpleNamespace

from doc2vec_completed.opensearch.db_store import SentenceVectorDBStore
from doc2vec_completed.opensearch.handlers.live_handler import LiveHandler
from doc2vec_completed.opensearch.sentence_splitter import split_document
from doc2vec_completed.opensearch.sqs_processor import SqsProcessor
from ta_db_connection.atables import SentencesVector, TextSimilarityInput
from tests.opensearch.fakes import OpenSearchFake, SessionFake, SessionSequence, SqsFake


class DocumentStoreFake:
    def __init__(self, document):
        self.document = document

    def load_document(self, tsp_id):
        return self.document


class LiveSentenceStoreFake:
    def __init__(self):
        self.calls = []
        self.next_global_id = 1

    def save_live_sentence_batch(self, document, sentences):
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
                tsp=document,
            )
            rows.append(row)
            self.next_global_id += 1
        return rows


class BatchProcessorFake:
    def __init__(self):
        self.calls = []

    def catalog_alias(self, analysis_group):
        return f"catalog-{analysis_group.lower()}"

    def process(self, rows, label):
        self.calls.append((rows, label))
        return len(rows)


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


def build_handler(service_config, custom_logger, document, sentences):
    opensearch = OpenSearchFake()
    sqs = SqsFake()
    handler = LiveHandler(
        service_config,
        SessionSequence([]),
        opensearch,
        sqs,
        custom_logger,
        sentence_splitter=lambda content: sentences,
    )
    handler.document_db_store = DocumentStoreFake(document)
    handler.sentence_vector_db_store = LiveSentenceStoreFake()
    handler.batch_processor = BatchProcessorFake()
    return handler, opensearch, sqs


def test_live_handler_saves_indexes_refreshes_and_publishes(
    service_config,
    custom_logger,
):
    sentences = ["First sentence.", "Second sentence."]
    handler, opensearch, sqs = build_handler(
        service_config,
        custom_logger,
        live_document(),
        sentences,
    )

    result = handler.process(10)

    assert result["indexed_count"] == 2
    assert handler.sentence_vector_db_store.calls == [
        [(1, sentences[0]), (2, sentences[1])]
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
        [],
    )

    result = handler.process(10)

    assert result["reason"] == "no_content"
    assert opensearch.refresh_alias_calls == []
    assert sqs.sent[0]["message_body"]["success"] is False


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


def test_live_sentence_store_reuses_local_ids_and_builds_new_rows():
    document = live_document()
    existing = SentencesVector(
        global_id=20,
        tsp_id=10,
        a_number=1,
        section_name="old",
        sent_id_local=1,
        sentence_content="Old text.",
        word_count=2,
        source_type="old",
        analysis_group="old",
        is_current=True,
        sentence_key="a" * 64,
    )
    session = SessionFake(rows=[existing])
    store = SentenceVectorDBStore(SessionSequence([session]))

    rows = store.save_live_sentence_batch(
        document,
        [(1, "Updated sentence."), (2, "New sentence.")],
    )

    assert [row.sent_id_local for row in rows] == [1, 2]
    assert rows[0].global_id == 20
    assert rows[1].source_type == "affidavit"
    assert all(len(row.sentence_key) == 64 for row in rows)
    assert session.commits == 1


def test_syntok_splitter_returns_original_text_and_handles_empty_content():
    content = "I arrived yesterday. Then I stayed with my aunt."

    assert split_document(content) == [
        "I arrived yesterday.",
        "Then I stayed with my aunt.",
    ]
    assert split_document(" ¶ \n") == []
