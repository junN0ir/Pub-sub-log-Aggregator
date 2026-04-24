def test_new_event_not_duplicate(store):
    assert store.is_duplicate("logs.app", "evt-100") is False

def test_detects_duplicate_after_mark(store):
    store.mark_processed("logs.app", "evt-200", "svc-a",
                         "2024-01-01T00:00:00Z", {"x": 1})
    assert store.is_duplicate("logs.app", "evt-200") is True

def test_mark_processed_idempotent(store):
    kwargs = dict(topic="logs.app", event_id="evt-300",
                  source="svc-b", timestamp="2024-01-01T01:00:00Z", payload={})
    assert store.mark_processed(**kwargs) is True
    assert store.mark_processed(**kwargs) is False