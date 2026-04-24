import time
import asyncio
import pytest
from src.dedup_store import DedupStore
from src.consumer import EventConsumer
from src.models import Event
from pathlib import Path

TEST_DB = Path("/tmp/test_dedup.db")

def test_stress_dedup_store():
    store = DedupStore(db_path=TEST_DB)
    start = time.time()
    for i in range(1000):
        store.mark_processed("stress.topic", f"stress-{i:04d}",
                             "stress-pub", "2024-01-01T00:00:00Z", {"seq": i})
    for i in range(500):  # 500 duplikat
        store.mark_processed("stress.topic", f"stress-{i:04d}",
                             "stress-pub", "2024-01-01T00:00:00Z", {"seq": i})
    elapsed = time.time() - start
    assert store.count_unique() == 1000
    assert elapsed < 10.0, f"Terlalu lambat: {elapsed:.2f}s"

@pytest.mark.asyncio
async def test_consumer_idempotency():
    store = DedupStore(db_path=TEST_DB)
    queue = asyncio.Queue()
    consumer = EventConsumer(queue, store)
    event = Event(topic="idem.test", event_id="idem-001",
                  timestamp="2024-01-01T12:00:00Z",
                  source="test-svc", payload={"data": "hello"})
    for _ in range(3):
        await queue.put(event)
    await consumer.start()
    await asyncio.sleep(0.5)
    await consumer.stop()
    stats = consumer.get_stats()
    assert stats["received"] == 3
    assert stats["unique_processed"] == 1
    assert stats["duplicate_dropped"] == 2