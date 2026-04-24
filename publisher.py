"""
Publisher script — untuk demo dan Docker Compose.
Mengirim 5000+ event (dengan 20%+ duplikasi) ke aggregator.
"""
import asyncio
import json
import random
import time
import uuid
from datetime import datetime, timezone

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    import urllib.request
    import urllib.error

AGGREGATOR_URL = __import__("os").environ.get("AGGREGATOR_URL", "http://localhost:8080")
TOTAL_UNIQUE = 5000
DUPLICATE_RATIO = 0.25  # 25% duplikat
BATCH_SIZE = 100
TOPICS = ["logs.app", "logs.db", "logs.auth", "logs.payment", "metrics.cpu"]


def make_event(topic: str, event_id: str = None) -> dict:
    return {
        "topic": topic,
        "event_id": event_id or f"evt-{uuid.uuid4().hex[:12]}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": f"publisher-{random.randint(1, 5)}",
        "payload": {
            "level": random.choice(["INFO", "WARN", "ERROR"]),
            "msg": f"Log message {random.randint(1000, 9999)}",
            "seq": random.randint(0, 999999),
        }
    }


def send_batch_sync(events: list[dict]) -> dict:
    """Kirim batch menggunakan urllib (built-in, tanpa httpx)."""
    url = f"{AGGREGATOR_URL}/publish"
    body = json.dumps({"events": events}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"  [ERROR] Gagal kirim: {e}")
        return {}


def wait_for_aggregator(max_wait: int = 60):
    """Tunggu sampai aggregator ready."""
    url = f"{AGGREGATOR_URL}/health"
    print(f"Menunggu aggregator di {url}...")
    for i in range(max_wait):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    print(f"Aggregator ready setelah {i+1} detik!")
                    return True
        except Exception:
            time.sleep(1)
    print("Aggregator tidak tersedia!")
    return False


def main():
    print("=" * 60)
    print("  Pub-Sub Log Aggregator — Publisher Demo")
    print("=" * 60)

    if not wait_for_aggregator():
        return

    # Generate unique event IDs
    unique_ids = [(random.choice(TOPICS), f"evt-{uuid.uuid4().hex[:12]}")
                  for _ in range(TOTAL_UNIQUE)]

    # Tambah duplikat (25%)
    dup_count = int(TOTAL_UNIQUE * DUPLICATE_RATIO)
    duplicates = [random.choice(unique_ids) for _ in range(dup_count)]

    all_events_meta = unique_ids + duplicates
    random.shuffle(all_events_meta)

    total = len(all_events_meta)
    print(f"\nTotal event yang akan dikirim: {total}")
    print(f"  Unique: {TOTAL_UNIQUE}")
    print(f"  Duplikat: {dup_count} ({DUPLICATE_RATIO*100:.0f}%)")
    print(f"  Batch size: {BATCH_SIZE}\n")

    start = time.time()
    sent = 0
    batch = []

    for topic, event_id in all_events_meta:
        batch.append(make_event(topic, event_id))
        if len(batch) >= BATCH_SIZE:
            result = send_batch_sync(batch)
            sent += len(batch)
            print(f"  Terkirim: {sent}/{total} | {result.get('message', '')}")
            batch.clear()

    if batch:
        send_batch_sync(batch)
        sent += len(batch)

    elapsed = time.time() - start
    print(f"\nSelesai! {sent} event dikirim dalam {elapsed:.2f}s")
    print(f"Throughput: {sent/elapsed:.1f} event/detik")

    # Cek stats akhir
    time.sleep(2)  # tunggu consumer selesai
    try:
        req = urllib.request.Request(f"{AGGREGATOR_URL}/stats")
        with urllib.request.urlopen(req, timeout=10) as resp:
            stats = json.loads(resp.read())
            print("\n=== STATS AKHIR ===")
            print(f"  Received:          {stats['received']}")
            print(f"  Unique Processed:  {stats['unique_processed']}")
            print(f"  Duplicate Dropped: {stats['duplicate_dropped']}")
            print(f"  Topics:            {stats['topics']}")
            print(f"  Uptime:            {stats['uptime_seconds']}s")
    except Exception as e:
        print(f"Gagal ambil stats: {e}")


if __name__ == "__main__":
    main()