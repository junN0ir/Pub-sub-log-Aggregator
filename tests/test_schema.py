import pytest
from src.models import Event
from pydantic import ValidationError

def test_event_schema_valid():
    event = Event(
        topic="logs.app", event_id="evt-001",
        timestamp="2024-01-01T00:00:00Z",
        source="service-a", payload={"level": "INFO"}
    )
    assert event.topic == "logs.app"
    assert event.event_id == "evt-001"

def test_event_schema_invalid_timestamp():
    with pytest.raises(ValidationError) as exc_info:
        Event(topic="logs.app", event_id="evt-002",
              timestamp="01-01-2024 00:00",
              source="service-a", payload={})
    assert "ISO8601" in str(exc_info.value)

def test_event_schema_empty_topic():
    with pytest.raises(ValidationError):
        Event(topic="", event_id="evt-003",
              timestamp="2024-01-01T00:00:00Z",
              source="service-a", payload={})

def test_event_schema_empty_source():
    with pytest.raises(ValidationError):
        Event(topic="logs.app", event_id="evt-004",
              timestamp="2024-01-01T00:00:00Z",
              source="", payload={})