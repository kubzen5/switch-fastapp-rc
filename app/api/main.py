from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.responses import JSONResponse

from app.api.models import Health, EventPage, EntityResponse, StatsResponse
from app.config import get_settings
from app.db.connection import connect
from app.logging import configure_logging
from app.runtime import check_postgres, wait_for_postgres


@asynccontextmanager
async def lifespan(_app: FastAPI):
    import asyncio

    settings = get_settings()
    configure_logging(settings.log_level)
    await asyncio.to_thread(wait_for_postgres, settings, True)
    yield


app = FastAPI(title="Switch Event Inspection", version="0.1.0", lifespan=lifespan)


@app.get("/health/live", response_model=Health)
def live():
    return {"status": "ok"}


@app.get("/health/ready", response_model=Health, responses={503: {"model": Health}})
def ready():
    try:
        check_postgres(get_settings())
    except Exception:
        return JSONResponse(status_code=503, content={"status": "unavailable"})
    return {"status": "ok"}


def validate_time_range(since, until):
    for value in (since, until):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise HTTPException(422, "Time filters require an explicit timezone")
    if since is not None and until is not None and since > until:
        raise HTTPException(422, "since must not exceed until")


def event_rows(connection, *, entity_key=None, source=None, event_type=None, since=None, until=None, after_id=0, limit=100):
    conditions, values = ["d.delivery_id > %s"], [after_id]
    for column, value in (("entity_key", entity_key), ("source", source), ("event_type", event_type)):
        if value is not None:
            conditions.append(f"d.{column} = %s")
            values.append(value)
    for operator, value in ((">=", since), ("<=", until)):
        if value is not None:
            conditions.append(f"d.occurred_at {operator} %s")
            values.append(value)
    return connection.execute("""
        SELECT d.delivery_id, d.event_id, d.source, d.entity_key, d.event_type,
               d.occurred_at, d.captured_at, d.processed_at, d.batch_id,
               d.topic, d.partition_id, d.broker_offset, d.disposition,
               p.envelope, r.reason_code
        FROM event_deliveries d
        LEFT JOIN processed_events p ON p.event_id = d.event_id AND d.disposition <> 'rejected'
        LEFT JOIN event_rejections r USING (delivery_id)
        WHERE """ + " AND ".join(conditions) + " ORDER BY d.delivery_id LIMIT %s",
        (*values, limit)).fetchall()


@app.get("/events", response_model=EventPage, description="Delivery audit ordered by delivery_id ASC. since/until inclusively filter occurred_at (source occurrence time); timezone required. Rejected messages without occurred_at do not match time filters.")
def events(
    entity_key: Annotated[str | None, Query(min_length=1, max_length=512)] = None, source: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
    event_type: Literal["insert", "update"] | None = None,
    since: datetime | None = None, until: datetime | None = None,
    after_id: Annotated[int, Query(ge=0, le=9223372036854775807)] = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
):
    validate_time_range(since, until)
    with connect(get_settings()) as connection:
        rows = event_rows(connection, entity_key=entity_key, source=source, event_type=event_type,
                          since=since, until=until, after_id=after_id, limit=limit + 1)
    return {"items": rows[:limit], "next_after_id": rows[limit - 1]["delivery_id"] if len(rows) > limit else None}


@app.get("/entities/{key}", response_model=EntityResponse, responses={404: {"description": "Entity not found"}})
def entity(key: Annotated[str, Path(min_length=1, max_length=512)], source: Annotated[str | None, Query(min_length=1, max_length=512)] = None, after_id: Annotated[int, Query(ge=0, le=9223372036854775807)] = 0,
           limit: Annotated[int, Query(ge=1, le=500)] = 100):
    source = source or get_settings().source_name
    with connect(get_settings()) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        current = connection.execute(
            "SELECT * FROM orders_current WHERE source = %s AND entity_key = %s", (source, key)
        ).fetchone()
        exists = current is not None or connection.execute(
            "SELECT 1 FROM event_deliveries WHERE source = %s AND entity_key = %s LIMIT 1", (source, key)
        ).fetchone() is not None
        history = event_rows(connection, entity_key=key, source=source, after_id=after_id, limit=limit + 1)
    if not exists:
        raise HTTPException(404, "Entity not found")
    if current is not None:
        current["total_price"] = format(current["total_price"], ".2f")
    return {"current": current, "history": history[:limit],
            "next_after_id": history[limit - 1]["delivery_id"] if len(history) > limit else None}


@app.get("/stats", response_model=StatsResponse)
def stats():
    with connect(get_settings()) as connection:
        # All counters and checkpoints describe one committed database snapshot.
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        counts = connection.execute("""
            SELECT event_type, count(*) AS deliveries,
                count(*) FILTER (WHERE disposition IN ('accepted', 'stale')) AS unique_events,
                count(*) FILTER (WHERE disposition = 'accepted') AS accepted,
                count(*) FILTER (WHERE disposition = 'stale') AS stale,
                count(*) FILTER (WHERE disposition = 'duplicate') AS duplicates,
                count(*) FILTER (WHERE disposition = 'rejected') AS rejected
            FROM event_deliveries GROUP BY event_type ORDER BY event_type NULLS LAST
        """).fetchall()
        delay = connection.execute("""
            SELECT count(*) AS sample_count,
                   avg(extract(epoch FROM d.processed_at - d.occurred_at)) AS avg_seconds,
                   min(extract(epoch FROM d.processed_at - d.occurred_at)) AS min_seconds,
                   max(extract(epoch FROM d.processed_at - d.occurred_at)) AS max_seconds
            FROM processed_events p JOIN event_deliveries d ON d.delivery_id = p.first_delivery_id
        """).fetchone()
        watermarks = connection.execute("""
            SELECT sources.source, s.last_updated_at, s.last_order_key,
                   s.last_completed_batch_id, s.updated_at AS checkpoint_persisted_at,
                   b.finished_at AS batch_completed_at,
                   (SELECT max(r.finished_at) FROM sync_runs r
                    WHERE r.source = sources.source AND r.status = 'completed') AS last_sync_completed_at
            FROM (SELECT source FROM sync_state UNION SELECT source FROM sync_runs) sources
            LEFT JOIN sync_state s USING (source)
            LEFT JOIN sync_batches b ON b.batch_id = s.last_completed_batch_id
            ORDER BY sources.source
        """).fetchall()
        rejected = connection.execute("SELECT count(*) AS count FROM source_quarantine").fetchone()["count"]
    totals = {key: sum(row[key] for row in counts) for key in
              ('deliveries', 'unique_events', 'accepted', 'stale', 'duplicates', 'rejected')}
    return {"counts": totals, "counts_by_type": counts,
            "adapter_rejected_records": rejected, "processing_delay": delay, "watermarks": watermarks}
