from pathlib import Path
from src.dedup_store import DedupStore

TEST_DB = Path("/tmp/test_dedup.db")

def test_persists_after_restart():
    store1 = DedupStore(db_path=TEST_DB)
    store1.mark_processed("logs.db", "persist-001", "db-svc",
                          "2024-06-01T10:00:00Z", {"msg": "persisted"})
    store2 = DedupStore(db_path=TEST_DB)
    assert store2.is_duplicate("logs.db", "persist-001") is True

def test_different_topic_same_event_id(store):
    store.mark_processed("topic-A", "same-id", "svc",
                         "2024-01-01T00:00:00Z", {})
    assert store.is_duplicate("topic-A", "same-id") is True
    assert store.is_duplicate("topic-B", "same-id") is False