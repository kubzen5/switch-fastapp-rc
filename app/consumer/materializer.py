import logging
from dataclasses import dataclass

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from app.domain.envelope import EventEnvelope


@dataclass(frozen=True)
class Delivery:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


def materialize(connection: psycopg.Connection, delivery: Delivery) -> str:
    """Persist one delivery; return only after the database commit succeeds."""
    # A nested transaction is only a savepoint: returning from it would allow
    # the caller to acknowledge Kafka before the outer transaction commits.
    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise RuntimeError("materialize requires an idle connection to own the commit")
    event = None
    reason = None
    details = []
    try:
        if delivery.value is None:
            raise ValueError("Tombstones are unsupported")
        event = EventEnvelope.model_validate_json(delivery.value)
        if delivery.key != event.partition_key.encode():
            reason = "partition_key_mismatch"
    except ValidationError as error:
        # Do not persist Pydantic input values or exception strings.
        details = [{"loc": list(item["loc"]), "type": item["type"]} for item in error.errors()]
        if any(item["type"] == "json_invalid" for item in details):
            reason = "invalid_json"
        elif any(item["loc"] and item["loc"][0] == "payload" for item in details):
            reason = "business_quality_violation"
        else:
            reason = "invalid_envelope"
    except ValueError:
        reason = "unsupported_tombstone"

    with connection.transaction():
        disposition = "rejected" if reason else "accepted"
        if event and not reason:
            # Serialize all versions of this entity across consumer processes.
            connection.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"{event.source}:{event.entity_key}",))
            previous = connection.execute(
                "SELECT content_hash FROM processed_events WHERE event_id = %s", (event.event_id,)
            ).fetchone()
            if previous:
                if previous["content_hash"] == event.content_hash():
                    disposition = "duplicate"
                else:
                    disposition, reason = "rejected", "source_version_conflict"
            else:
                current = connection.execute(
                    "SELECT source_version FROM orders_current WHERE source = %s AND entity_key = %s",
                    (event.source, event.entity_key),
                ).fetchone()
                if current and event.source_version <= current["source_version"]:
                    disposition = "stale"

        row = connection.execute("""
            INSERT INTO event_deliveries (
                topic, partition_id, broker_offset, message_key, raw_message,
                event_id, source, entity_key, event_type, occurred_at, captured_at, batch_id, disposition
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING delivery_id
        """, (
            delivery.topic, delivery.partition, delivery.offset, delivery.key, delivery.value,
            event.event_id if event else None, event.source if event else None,
            event.entity_key if event else None, event.event_type if event else None,
            event.occurred_at if event else None, event.captured_at if event else None,
            event.batch_id if event else None, disposition,
        )).fetchone()

        if reason:
            connection.execute(
                "INSERT INTO event_rejections(delivery_id, reason_code, details) VALUES (%s, %s, %s)",
                (row["delivery_id"], reason, Jsonb(details)),
            )
        elif disposition in ("accepted", "stale"):
            connection.execute("""
                INSERT INTO processed_events (event_id, source, entity_key, source_version, content_hash, envelope, first_delivery_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (event.event_id, event.source, event.entity_key, event.source_version,
                  event.content_hash(), Jsonb(event.model_dump(mode="json")), row["delivery_id"]))
            connection.execute("""
                INSERT INTO orders_current (source, entity_key, source_version, source_updated_at,
                    payload, total_price, last_event_id, last_captured_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, entity_key) DO UPDATE SET
                    source_version = EXCLUDED.source_version,
                    source_updated_at = EXCLUDED.source_updated_at,
                    payload = EXCLUDED.payload, total_price = EXCLUDED.total_price,
                    last_event_id = EXCLUDED.last_event_id, last_captured_at = EXCLUDED.last_captured_at,
                    updated_at = now()
                WHERE EXCLUDED.source_version > orders_current.source_version
            """, (event.source, event.entity_key, event.source_version, event.source_updated_at,
                  Jsonb(event.payload.model_dump(mode="json")), event.payload.total_price,
                  event.event_id, event.captured_at))
    correlation = {"topic": delivery.topic, "partition": delivery.partition, "offset": delivery.offset}
    if event:
        correlation.update(event_id=event.event_id, batch_id=str(event.batch_id),
                           source=event.source, entity_key=event.entity_key)
    logging.getLogger(__name__).info("delivery_" + disposition, extra=correlation)
    return disposition
