import struct
from types import SimpleNamespace

import pytest

from doc2vec_service.errors import PermanentDataError
from doc2vec_service.handlers.tracer_handler import TracerHandler
from tests.fakes import OpenSearchFake, SessionFake, SessionSequence


def test_tracer_handler_reads_and_indexes_every_row(serviceConfig):
    row = SimpleNamespace(
        tracer_sentence_id=7,
        sentence_content="This is standard form language.",
        is_non_matcher=True,
        is_tracer=False,
        is_form_language=True,
        analysis_group="Refugee",
        embedding=struct.pack("<2f", 0.25, 0.75),
    )
    sessions = SessionSequence(
        [
            SessionFake(scalarValue=1),
            SessionFake(rows=[row]),
            SessionFake(rows=[]),
        ]
    )
    opensearch = OpenSearchFake()
    handler = TracerHandler(serviceConfig, sessions, opensearch)

    result = handler.process()

    assert result["indexedCount"] == 1
    assert opensearch.bulkCalls[0][0] == "tracer"
    document = opensearch.bulkCalls[0][1][0]
    assert document["analysisGroup"] == "Refugee"
    assert document["sentenceContentVector"] == pytest.approx([0.25, 0.75])


def test_tracer_handler_validates_saved_vector_bytes(serviceConfig):
    handler = TracerHandler(serviceConfig, SessionSequence([]), OpenSearchFake())
    assert handler._decodeVector(struct.pack("<2f", 0.1, 0.2)) == pytest.approx(
        [0.1, 0.2]
    )
    with pytest.raises(PermanentDataError):
        handler._decodeVector("not bytes")
    with pytest.raises(PermanentDataError):
        handler._decodeVector(b"short")
