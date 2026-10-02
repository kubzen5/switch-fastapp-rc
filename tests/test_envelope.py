import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.domain.envelope import EventEnvelope, stable_event_id


def example(**changes):
    values = {
        "event_id": stable_event_id("snowflake.orders_source", "order:123", 1),
        "event_type": "insert", "source": "snowflake.orders_source", "entity_key": "order:123",
        "source_version": 1, "source_updated_at": "2026-09-30T10:00:00.123456Z",
        "occurred_at": "2026-09-30T12:00:00.123456+02:00", "captured_at": "2026-09-30T10:00:03Z",
        "batch_id": uuid4(), "sync_run_id": uuid4(), "batch_sequence": 1, "schema_version": 1,
        "payload": {"order_key": 123, "customer_key": 456, "customer_name": "Sample customer",
                    "order_status": "O", "total_price": "100.20", "order_date": "2026-09-01"},
    }
    values.update(changes)
    return values


def test_republication_keeps_identity_and_content_hash():
    first = EventEnvelope(**example())
    second = first.model_copy(update={"captured_at": datetime.now(timezone.utc), "batch_id": uuid4(), "sync_run_id": uuid4()})
    assert second.event_id == first.event_id
    assert second.content_hash() == first.content_hash()
    assert stable_event_id(first.source, first.entity_key, 2) != first.event_id
    assert second.partition_key == "order:123"


def test_wire_roundtrip_preserves_money_and_microseconds():
    event = EventEnvelope(**example())
    wire = json.loads(event.model_dump_json())
    assert wire["payload"]["total_price"] == "100.20"
    assert wire["occurred_at"] == "2026-09-30T10:00:00.123456Z"
    assert EventEnvelope.model_validate_json(event.model_dump_json()) == event


@pytest.mark.parametrize("changes", [
    {"occurred_at": "2026-09-30T10:00:00.123456"},
    {"occurred_at": 1790762400}, {"captured_at": 1790762403.0},
    {"event_type": "delete"}, {"source_version": 0}, {"source_version": True},
    {"schema_version": 2}, {"schema_version": True}, {"event_id": "a" * 64},
    {"entity_key": "order:124"}, {"unexpected": "field"},
])
def test_invalid_envelope_is_rejected(changes):
    with pytest.raises(ValidationError):
        EventEnvelope(**example(**changes))


@pytest.mark.parametrize("price", ["-0.01", "1.001", "NaN", "Infinity", 1.2, True, None, "10000000000000000.00"])
def test_bad_money_is_rejected(price):
    values = example()
    values["payload"]["total_price"] = price
    with pytest.raises(ValidationError):
        EventEnvelope(**values)


def test_reusing_version_with_different_content_changes_content_hash():
    values = example()
    first = EventEnvelope(**values)
    values["payload"]["total_price"] = "101.20"
    second = EventEnvelope(**values)
    assert first.event_id == second.event_id
    assert first.content_hash() != second.content_hash()
