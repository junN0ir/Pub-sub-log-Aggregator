import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse

from src.models import Event, PublishRequest, StatsResponse
from src.dedup_store import DedupStore
from src.queue_manager import get_queue, enqueue_events
from src.consumer import EventConsumer

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Globals ──────────────────────────────────────────────────────────────────
START_TIME = time.time()
dedup_store: DedupStore = None
consumer: EventConsumer = None


# ─── Lifespan ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global dedup_store, consumer

    logger.info("=== Aggregator starting up ===")
    dedup_store = DedupStore()
    queue = get_queue()
    consumer = EventConsumer(queue, dedup_store)
    await consumer.start()

    # Sinkronkan counter consumer dengan data yang sudah ada di DB (setelah restart)
    existing_count = dedup_store.count_unique()
    consumer.unique_processed = existing_count
    logger.info(f"Restored {existing_count} previously processed events from dedup store.")

    yield

    logger.info("=== Aggregator shutting down ===")
    await consumer.stop()


# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description="Idempotent consumer dengan persistent deduplication store.",
    version="1.0.0",
    lifespan=lifespan,
)


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.post("/publish", status_code=202)
async def publish(request: PublishRequest):
    """
    Terima batch event dari publisher.
    Event dimasukkan ke internal asyncio.Queue untuk diproses consumer.
    """
    if not request.events:
        raise HTTPException(status_code=400, detail="Tidak ada event dalam request.")

    enqueued = await enqueue_events(request.events)

    return {
        "status": "accepted",
        "total_submitted": len(request.events),
        "enqueued": enqueued,
        "message": f"{enqueued} event diterima untuk diproses."
    }


@app.post("/publish/single", status_code=202)
async def publish_single(event: Event):
    """Endpoint alternatif untuk single event."""
    enqueued = await enqueue_events([event])
    return {
        "status": "accepted",
        "enqueued": enqueued,
    }


@app.get("/events")
async def get_events(topic: str = Query(..., description="Nama topic yang ingin dilihat")):
    """
    Kembalikan daftar event unik yang sudah diproses untuk topic tertentu.
    """
    events = dedup_store.get_events_by_topic(topic)
    return {
        "topic": topic,
        "count": len(events),
        "events": events,
    }


@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """
    Tampilkan statistik sistem:
    received, unique_processed, duplicate_dropped, topics, uptime.
    """
    stats = consumer.get_stats()
    topics = dedup_store.get_all_topics()
    uptime = time.time() - START_TIME

    return StatsResponse(
        received=stats["received"],
        unique_processed=stats["unique_processed"],
        duplicate_dropped=stats["duplicate_dropped"],
        topics=topics,
        uptime_seconds=round(uptime, 2),
    )


@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/")
async def root():
    return {
        "service": "Pub-Sub Log Aggregator",
        "version": "1.0.0",
        "endpoints": ["/publish", "/publish/single", "/events?topic=...", "/stats", "/health"]
    }