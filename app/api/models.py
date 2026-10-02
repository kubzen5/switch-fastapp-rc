"""Public inspection contract; audit deliveries and domain events are distinct."""
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel


class Health(BaseModel):
    status: Literal['ok', 'unavailable']


class EventDelivery(BaseModel):
    delivery_id: int
    event_id: str | None
    source: str | None
    entity_key: str | None
    event_type: str | None
    occurred_at: datetime | None
    captured_at: datetime | None
    processed_at: datetime
    batch_id: UUID | None
    topic: str
    partition_id: int
    broker_offset: int
    disposition: Literal['accepted', 'stale', 'duplicate', 'rejected']
    envelope: dict[str, Any] | None
    reason_code: str | None


class EventPage(BaseModel):
    items: list[EventDelivery]
    next_after_id: int | None


class CurrentEntity(BaseModel):
    source: str
    entity_key: str
    source_version: int
    source_updated_at: datetime
    payload: dict[str, Any]
    total_price: str
    last_event_id: str
    last_captured_at: datetime
    updated_at: datetime


class EntityResponse(BaseModel):
    current: CurrentEntity | None
    history: list[EventDelivery]
    next_after_id: int | None


class Counts(BaseModel):
    deliveries: int
    unique_events: int
    accepted: int
    stale: int
    duplicates: int
    rejected: int


class TypeCounts(Counts):
    event_type: str | None


class ProcessingDelay(BaseModel):
    unit: Literal['seconds'] = 'seconds'
    scope: Literal['first_delivery_of_unique_events'] = 'first_delivery_of_unique_events'
    sample_count: int
    avg_seconds: float | None
    min_seconds: float | None
    max_seconds: float | None


class Watermark(BaseModel):
    source: str
    last_updated_at: datetime | None
    last_order_key: int | None
    last_completed_batch_id: UUID | None
    checkpoint_persisted_at: datetime | None
    batch_completed_at: datetime | None
    last_sync_completed_at: datetime | None


class StatsResponse(BaseModel):
    counts: Counts
    counts_by_type: list[TypeCounts]
    adapter_rejected_records: int
    processing_delay: ProcessingDelay
    watermarks: list[Watermark]
