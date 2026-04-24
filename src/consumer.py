import asyncio
import logging
import json
from src.dedup_store import DedupStore
from src.models import Event

logger = logging.getLogger(__name__)


class EventConsumer:
    """
    Idempotent consumer: memproses event dari asyncio.Queue.
    Setiap event dicek dulu ke DedupStore — jika sudah ada, drop.
    """

    def __init__(self, queue: asyncio.Queue, dedup_store: DedupStore):
        self.queue = queue
        self.dedup_store = dedup_store
        self.received = 0
        self.unique_processed = 0
        self.duplicate_dropped = 0
        self._running = False
        self._task: asyncio.Task = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._consume_loop())
        logger.info("EventConsumer started.")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("EventConsumer stopped.")

    async def _consume_loop(self):
        while self._running:
            try:
                event: Event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                await self._process_event(event)
                self.queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error saat memproses event: {e}")

    async def _process_event(self, event: Event):
        self.received += 1

        # Cek duplikat di DedupStore (blocking I/O, jalankan di executor)
        loop = asyncio.get_event_loop()
        is_dup = await loop.run_in_executor(
            None,
            self.dedup_store.is_duplicate,
            event.topic,
            event.event_id
        )

        if is_dup:
            self.duplicate_dropped += 1
            logger.warning(
                f"[DUPLICATE DROPPED] topic={event.topic} "
                f"event_id={event.event_id} source={event.source}"
            )
            return

        # Bukan duplikat — proses dan simpan
        success = await loop.run_in_executor(
            None,
            self.dedup_store.mark_processed,
            event.topic,
            event.event_id,
            event.source,
            event.timestamp,
            event.payload
        )

        if success:
            self.unique_processed += 1
            logger.info(
                f"[PROCESSED] topic={event.topic} "
                f"event_id={event.event_id} source={event.source}"
            )
        else:
            # Race condition: sudah diproses oleh worker lain
            self.duplicate_dropped += 1
            logger.warning(
                f"[RACE DUPLICATE] topic={event.topic} "
                f"event_id={event.event_id}"
            )

    def get_stats(self) -> dict:
        return {
            "received": self.received,
            "unique_processed": self.unique_processed,
            "duplicate_dropped": self.duplicate_dropped,
        }