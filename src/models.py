from pydantic import BaseModel, field_validator
from typing import Any, Optional
from datetime import datetime


class EventPayload(BaseModel):
    model_config = {"extra": "allow"}


class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: dict[str, Any] = {}

    @field_validator("topic")
    @classmethod
    def topic_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("topic tidak boleh kosong")
        return v.strip()

    @field_validator("event_id")
    @classmethod
    def event_id_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("event_id tidak boleh kosong")
        return v.strip()

    @field_validator("timestamp")
    @classmethod
    def timestamp_valid(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("timestamp harus format ISO8601 (contoh: 2024-01-01T00:00:00Z)")
        return v

    @field_validator("source")
    @classmethod
    def source_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("source tidak boleh kosong")
        return v.strip()


class PublishRequest(BaseModel):
    events: list[Event]


class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: list[str]
    uptime_seconds: float


class EventResponse(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: dict[str, Any]
    processed_at: str