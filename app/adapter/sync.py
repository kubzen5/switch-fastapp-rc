"""Immutable journal paging; publication precedes atomic batch checkpoint."""
from datetime import datetime, timezone
from uuid import uuid4

from pydantic import ValidationError
from app.domain.cursor import SyncCursor
from app.domain.envelope import EventEnvelope, stable_event_id


def event_from_row(row, source, batch_id, run_id, sequence):
    return EventEnvelope(
        event_id=stable_event_id(source, f"order:{row['order_key']}", row['source_version']),
        event_type=row['event_type'], source=source, entity_key=f"order:{row['order_key']}",
        payload={key: row[key] for key in ('order_key', 'customer_key', 'customer_name', 'order_status', 'total_price', 'order_date')},
        occurred_at=row['source_updated_at'], source_updated_at=row['source_updated_at'],
        captured_at=datetime.now(timezone.utc), source_version=row['source_version'],
        batch_id=batch_id, sync_run_id=run_id, batch_sequence=sequence,
    )


def run_sync(reader, store, publisher, source, batch_size, pulse=lambda: None):
    after = store.load(source)
    high = reader.high_watermark()
    if after is not None and (high is None or after.follows(high)):
        raise RuntimeError('Source journal regressed behind durable checkpoint')
    if high is None or high == after:
        return 0
    run_id = store.start_run(source, after, high)
    sequence, total = 0, 0
    try:
        while True:
            rows = reader.read_batch(after, high, batch_size)
            if not rows:
                break
            sequence += 1
            end = SyncCursor(source_updated_at=rows[-1]['source_updated_at'], order_key=rows[-1]['order_key'])
            previous = after
            for row in rows:
                current = SyncCursor(source_updated_at=row['source_updated_at'], order_key=row['order_key'])
                if (previous and not current.follows(previous)) or current.follows(high):
                    raise RuntimeError('Source violated cursor order / uniqueness')
                previous = current
            batch_id = store.start_batch(run_id, sequence, after, end, len(rows))
            events, rejected = [], []
            for row in rows:
                pulse()
                try:
                    event = event_from_row(row, source, batch_id, run_id, sequence)
                except ValidationError as error:
                    rejected.append((row, str(error)))
                    continue
                publisher.publish(event)  # Must return only after ACK.
                events.append(event)
            pulse()  # Stop before checkpoint: replay the uncommitted batch.
            store.commit_batch(source, batch_id, end, events, rejected)
            after = end
            total += len(rows)
        if after != high:
            raise RuntimeError('Journal gap: high watermark not reached')
        store.finish_run(run_id, 'completed')
    except Exception:
        store.finish_run(run_id, 'failed')
        raise
    return total
