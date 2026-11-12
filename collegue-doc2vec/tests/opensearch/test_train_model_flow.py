from types import SimpleNamespace

from doc2vec_completed.opensearch.handlers.train_model_handler import TrainModelHandler
from doc2vec_completed.opensearch.sqs_processor import SqsProcessor
from doc2vec_completed.opensearch.text import sentence_key
from ta_db_connection.atatables import Embeddings, SentencesVector
from tests.opensearch.fakes import OpenSearchFake, SessionFake, SessionSequence, SqsFake


def training_sentence(source, global_id, content):
    values = {
        column.key: getattr(source, column.key)
        for column in SentencesVector.__table__.columns
    }
    values.update(
        global_id=global_id,
        sent_id_local=global_id,
        sentence_content=content,
        sentence_key=None,
    )
    return SentencesVector(**values, tsp=source.tsp)


def test_train_model_reuses_vectors_and_indexes_only_unique_missing_keys(
    service_config,
    sentence_record,
    custom_logger,
):
    saved_text = "A saved Titan vector is available."
    bedrock_text = "Bedrock must create this vector."
    existing_text = "This training key already exists."
    eligible_rows = [
        training_sentence(sentence_record, 1, saved_text),
        training_sentence(sentence_record, 2, saved_text),
        training_sentence(sentence_record, 3, bedrock_text),
        training_sentence(sentence_record, 4, existing_text),
    ]
    non_matcher = training_sentence(sentence_record, 5, "Excluded non-matcher.")
    non_matcher.is_non_matcher = True
    tracer = training_sentence(sentence_record, 6, "Excluded tracer.")
    tracer.is_tracer = True
    form_language = training_sentence(sentence_record, 7, "Excluded form language.")
    form_language.is_form_language = True
    short_sentence = training_sentence(sentence_record, 8, "Too short")
    rows = [
        *eligible_rows,
        non_matcher,
        tracer,
        form_language,
        short_sentence,
    ]
    embeddings = [
        Embeddings(global_id=2, embedding_type=2, embeddings=[0.1, 0.2]),
        Embeddings(global_id=3, embedding_type=2, embeddings=[0.1]),
    ]
    load_session = SessionFake(rows=rows)
    sessions = SessionSequence(
        [
            SessionFake(scalar_value=len(eligible_rows)),
            load_session,
            SessionFake(rows=embeddings),
            SessionFake(rows=[]),
        ]
    )
    opensearch = OpenSearchFake(existing={sentence_key(existing_text)})
    handler = TrainModelHandler(sessions, opensearch, custom_logger)

    handler.process(10)

    written_documents = [
        document
        for alias, documents, _ in opensearch.bulk_calls
        if alias == "training"
        for document in documents
    ]
    assert {
        document["sentenceContent"] for document in written_documents
    } == {saved_text, bedrock_text}
    assert all("sentenceKey" not in document for document in written_documents)


class HandlerFake:
    def __init__(self):
        self.calls = []

    def process(self, value):
        self.calls.append(value)
        return {"indexed_count": 1}


def test_sqs_processor_routes_train_model_message(custom_logger):
    source_queue = SqsFake()
    train_handler = HandlerFake()
    dependencies = SimpleNamespace(
        aws_sqs=source_queue,
        logger=custom_logger,
        historical_handler=HandlerFake(),
        train_model_handler=train_handler,
        tracer_handler=HandlerFake(),
    )
    processor = SqsProcessor(dependencies)
    message = {
        "reason": "TRAIN_MODEL",
        "tsp_id": 10,
        "ReceiptHandle": "train-receipt",
    }

    processor._process_message(message)

    assert train_handler.calls == [10]

