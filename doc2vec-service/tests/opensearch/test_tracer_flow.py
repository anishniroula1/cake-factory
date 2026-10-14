from array import array
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from doc2vec_completed.opensearch.errors import PermanentDataError
from doc2vec_completed.opensearch.handlers.tracer_handler import TracerHandler
from doc2vec_completed.opensearch.sqs_processor import SqsProcessor
from ta_db_connection.atables import TracerSentence
from tests.opensearch.fakes import OpenSearchFake, SessionFake, SessionSequence, SqsFake


def test_tracer_handler_reuses_saved_vectors_and_creates_missing_vectors(
    service_config,
    custom_logger,
):
    now = datetime.now(UTC)
    rows = [
        TracerSentence(
            tracer_sentence_id=10,
            sentence_content="Saved tracer vector.",
            analysis_group="Asylee",
            is_non_matcher=True,
            is_form_language=False,
            embedding=[0.1, 0.2],
            last_modified=now,
        ),
        TracerSentence(
            tracer_sentence_id=20,
            sentence_content="Raw tracer vector.",
            analysis_group="Refugee",
            is_non_matcher=False,
            is_form_language=True,
            embedding=array("f", [0.3, 0.4]).tobytes(),
            last_modified=now,
        ),
        TracerSentence(
            tracer_sentence_id=30,
            sentence_content="Bedrock tracer vector.",
            analysis_group="Asylee",
            is_non_matcher=False,
            is_form_language=False,
            embedding=None,
            last_modified=now,
        ),
    ]
    opensearch = OpenSearchFake()
    handler = TracerHandler(
        service_config,
        SessionSequence([SessionFake(rows=rows)]),
        opensearch,
        custom_logger,
    )

    result = handler.process([10, 20, 30, 10])

    assert result == {"reason": "TRACER", "source_count": 3, "indexed_count": 3}
    calls = [call for call in opensearch.bulk_calls if call[1]]
    assert [(call[2], len(call[1])) for call in calls] == [(False, 2), (True, 1)]
    assert {document["tracerId"] for call in calls for document in call[1]} == {
        10,
        20,
        30,
    }
    assert opensearch.refresh_alias_calls == ["tracer"]


def test_tracer_handler_stops_when_a_requested_database_row_is_missing(
    service_config,
    custom_logger,
):
    handler = TracerHandler(
        service_config,
        SessionSequence([SessionFake(rows=[])]),
        OpenSearchFake(),
        custom_logger,
    )

    with pytest.raises(PermanentDataError, match="99"):
        handler.process([99])


class HandlerFake:
    def __init__(self):
        self.calls = []

    def process(self, value):
        self.calls.append(value)
        return {"indexed_count": 1}


def test_sqs_processor_routes_tracer_message(custom_logger):
    source_queue = SqsFake()
    tracer_handler = HandlerFake()
    dependencies = SimpleNamespace(
        aws_sqs=source_queue,
        logger=custom_logger,
        historical_handler=HandlerFake(),
        train_model_handler=HandlerFake(),
        tracer_handler=tracer_handler,
    )
    processor = SqsProcessor(dependencies)
    message = {
        "reason": "TRACER",
        "tracer_sentence_ids": [30, 20, 30],
        "ReceiptHandle": "tracer-receipt",
    }

    processor._process_message(message)

    assert tracer_handler.calls == [[30, 20]]
    assert source_queue.deleted == [message]
