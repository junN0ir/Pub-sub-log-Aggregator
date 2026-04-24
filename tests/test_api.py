import pytest
import pytest_asyncio
import asyncio
from pathlib import Path
from unittest.mock import patch
from httpx import AsyncClient, ASGITransport

TEST_DB = Path("/tmp/test_dedup_api.db")


@pytest_asyncio.fixture
async def client():
    if TEST_DB.exists():
        TEST_DB.unlink()

    with patch("src.dedup_store.DB_PATH", TEST_DB):
        # Import ulang agar patch DB_PATH efektif
        import importlib
        import src.dedup_store as ds_mod
        import src.main as main_mod

        importlib.reload(ds_mod)
        importlib.reload(main_mod)

        from src.dedup_store import DedupStore
        from src.queue_manager import get_queue
        from src.consumer import EventConsumer

        # Inisialisasi manual seperti lifespan
        dedup = DedupStore(db_path=TEST_DB)
        queue = get_queue()
        cons = EventConsumer(queue, dedup)
        await cons.start()

        existing = dedup.count_unique()
        cons.unique_processed = existing

        # Inject ke main module
        main_mod.dedup_store = dedup
        main_mod.consumer = cons

        transport = ASGITransport(app=main_mod.app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac

        await cons.stop()

    if TEST_DB.exists():
        TEST_DB.unlink()


@pytest.mark.asyncio
async def test_health_endpoint(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_publish_and_stats_consistent(client):
    """GET /stats harus konsisten dengan jumlah event yang dikirim."""
    events = [
        {"topic": "api.test", "event_id": f"api-{i:03d}",
         "timestamp": "2024-01-01T00:00:00Z",
         "source": "test", "payload": {}}
        for i in range(5)
    ]
    resp = await client.post("/publish", json={"events": events})
    assert resp.status_code == 202
    assert resp.json()["enqueued"] == 5

    await asyncio.sleep(1.0)  # tunggu consumer selesai

    stats = (await client.get("/stats")).json()
    assert stats["unique_processed"] >= 5
    assert stats["duplicate_dropped"] == 0


@pytest.mark.asyncio
async def test_publish_duplicate_reflected_in_stats(client):
    """Duplikat harus muncul di duplicate_dropped pada /stats."""
    event = {"topic": "dup.test", "event_id": "dup-fixed-001",
             "timestamp": "2024-01-01T00:00:00Z",
             "source": "test", "payload": {}}

    for _ in range(3):
        await client.post("/publish", json={"events": [event]})

    await asyncio.sleep(1.0)

    stats = (await client.get("/stats")).json()
    assert stats["duplicate_dropped"] >= 2


@pytest.mark.asyncio
async def test_get_events_consistent_with_publish(client):
    """GET /events harus mengembalikan event yang sudah dipublish."""
    events = [
        {"topic": "events.topic", "event_id": f"ev-{i}",
         "timestamp": "2024-01-01T00:00:00Z",
         "source": "test", "payload": {"n": i}}
        for i in range(3)
    ]
    await client.post("/publish", json={"events": events})
    await asyncio.sleep(1.0)

    resp = await client.get("/events", params={"topic": "events.topic"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 3
    assert data["topic"] == "events.topic"


@pytest.mark.asyncio
async def test_publish_empty_batch_rejected(client):
    resp = await client.post("/publish", json={"events": []})
    assert resp.status_code == 400