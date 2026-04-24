import sqlite3
import threading
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path("/app/data/dedup.db")


class DedupStore:
    """
    Persistent dedup store menggunakan SQLite.
    Thread-safe dengan threading.Lock.
    Tahan restart container selama volume di-mount.
    """

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self):
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS processed_events (
                        topic       TEXT NOT NULL,
                        event_id    TEXT NOT NULL,
                        source      TEXT NOT NULL,
                        timestamp   TEXT NOT NULL,
                        payload     TEXT NOT NULL,
                        processed_at TEXT NOT NULL,
                        PRIMARY KEY (topic, event_id)
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_topic ON processed_events(topic)
                """)
                conn.commit()
                logger.info(f"DedupStore initialized at {self.db_path}")
            finally:
                conn.close()

    def is_duplicate(self, topic: str, event_id: str) -> bool:
        """Return True jika event sudah pernah diproses."""
        with self._lock:
            conn = self._get_conn()
            try:
                cur = conn.execute(
                    "SELECT 1 FROM processed_events WHERE topic=? AND event_id=?",
                    (topic, event_id)
                )
                return cur.fetchone() is not None
            finally:
                conn.close()

    def mark_processed(self, topic: str, event_id: str, source: str,
                       timestamp: str, payload: str) -> bool:
        """
        Tandai event sebagai processed.
        Return True jika berhasil insert (baru), False jika duplikat (sudah ada).
        """
        import json
        processed_at = datetime.now(timezone.utc).isoformat()
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO processed_events
                       (topic, event_id, source, timestamp, payload, processed_at)
                       VALUES (?,?,?,?,?,?)""",
                    (topic, event_id, source, timestamp,
                     json.dumps(payload) if isinstance(payload, dict) else payload,
                     processed_at)
                )
                conn.commit()
                # Cek apakah insert berhasil (rowcount > 0 berarti baru)
                cur = conn.execute(
                    "SELECT processed_at FROM processed_events WHERE topic=? AND event_id=?",
                    (topic, event_id)
                )
                row = cur.fetchone()
                return row is not None and row[0] == processed_at
            finally:
                conn.close()

    def get_events_by_topic(self, topic: str) -> list[dict]:
        """Ambil semua event unik berdasarkan topic."""
        import json
        with self._lock:
            conn = self._get_conn()
            try:
                cur = conn.execute(
                    """SELECT topic, event_id, source, timestamp, payload, processed_at
                       FROM processed_events WHERE topic=?
                       ORDER BY processed_at ASC""",
                    (topic,)
                )
                rows = cur.fetchall()
                result = []
                for row in rows:
                    try:
                        payload = json.loads(row[4])
                    except Exception:
                        payload = {}
                    result.append({
                        "topic": row[0],
                        "event_id": row[1],
                        "source": row[2],
                        "timestamp": row[3],
                        "payload": payload,
                        "processed_at": row[5],
                    })
                return result
            finally:
                conn.close()

    def get_all_topics(self) -> list[str]:
        """Ambil semua topic yang ada."""
        with self._lock:
            conn = self._get_conn()
            try:
                cur = conn.execute(
                    "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
                )
                return [row[0] for row in cur.fetchall()]
            finally:
                conn.close()

    def count_unique(self) -> int:
        """Hitung total event unik yang sudah diproses."""
        with self._lock:
            conn = self._get_conn()
            try:
                cur = conn.execute("SELECT COUNT(*) FROM processed_events")
                return cur.fetchone()[0]
            finally:
                conn.close()

    def reset(self):
        """Hapus semua data (untuk testing)."""
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("DELETE FROM processed_events")
                conn.commit()
            finally:
                conn.close()