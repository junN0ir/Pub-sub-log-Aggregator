import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

TEST_DB = Path("/tmp/test_dedup.db")

@pytest.fixture(autouse=True)
def clean_db():
    if TEST_DB.exists():
        TEST_DB.unlink()
    yield
    if TEST_DB.exists():
        TEST_DB.unlink()

@pytest.fixture
def store():
    from src.dedup_store import DedupStore
    return DedupStore(db_path=TEST_DB)