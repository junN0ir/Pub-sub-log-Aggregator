import asyncio
import logging
from src.models import Event

logger = logging.getLogger(__name__)

# Global internal queue (asyncio-based)
_event_queue: asyncio.Queue = None


def get_queue() -> asyncio.Queue:
    global _event_queue
    if _event_queue is None:
        _event_queue = asyncio.Queue(maxsize=50000)
    return _event_queue


async def enqueue_events(events: list[Event]) -> int:
    """Masukkan events ke queue. Return jumlah yang berhasil di-enqueue."""
    q = get_queue()
    count = 0
    for event in events:
        try:
            await asyncio.wait_for(q.put(event), timeout=2.0)
            count += 1
        except asyncio.TimeoutError:
            logger.warning(f"Queue penuh! Event {event.event_id} dibuang.")
    return count