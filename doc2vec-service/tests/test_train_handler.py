from types import SimpleNamespace

from doc2vec_service.handlers.train_handler import TrainHandler
from tests.fakes import OpenSearchFake, SessionFake, SessionSequence


def test_train_handler_saves_keys_and_reuses_embeddings(
    serviceConfig,
    sentenceRecord,
):
    embedding = SimpleNamespace(global_id=1, embedding=[0.1, 0.2])
    saveSession = SessionFake()
    sessions = SessionSequence(
        [
            SessionFake(scalarValue=1),
            SessionFake(rows=[sentenceRecord]),
            saveSession,
            SessionFake(rows=[embedding]),
            SessionFake(rows=[]),
        ]
    )
    opensearch = OpenSearchFake()
    handler = TrainHandler(serviceConfig, sessions, opensearch)

    result = handler.process(10)

    assert result["sourceCount"] == 1
    assert result["indexedCount"] == 1
    assert len(sentenceRecord.sentence_key) == 64
    assert saveSession.commits == 1
    alias, documents, needsEmbedding = opensearch.bulkCalls[0]
    assert alias == "training"
    assert not needsEmbedding
    assert documents[0]["sentenceContentVector"] == [0.1, 0.2]


def test_train_handler_uses_bedrock_only_when_vector_is_missing(
    serviceConfig,
    sentenceRecord,
):
    sentenceRecord.sentence_key = "b" * 64
    opensearch = OpenSearchFake()
    handler = TrainHandler(serviceConfig, SessionSequence([SessionFake(rows=[])]), opensearch)

    assert handler._processWorker([sentenceRecord]) == 1
    assert opensearch.bulkCalls[-1][2] is True
    assert "sentenceContentVector" not in opensearch.bulkCalls[-1][1][0]


def test_train_handler_skips_existing_catalog_key(serviceConfig, sentenceRecord):
    opensearch = OpenSearchFake(existing={sentenceRecord.sentence_key})
    handler = TrainHandler(serviceConfig, SessionSequence([SessionFake(rows=[])]), opensearch)
    assert handler._processWorker([sentenceRecord]) == 0
