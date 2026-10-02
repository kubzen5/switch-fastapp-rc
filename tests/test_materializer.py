import os
import time
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.consumer.materializer import Delivery, materialize
from app.db.connection import connect
from app.domain.envelope import EventEnvelope, stable_event_id
from tests.test_envelope import example

pytestmark = pytest.mark.integration


@pytest.fixture
def database():
    password = os.environ.get("TEST_POSTGRES_PASSWORD")
    if not password:
        pytest.skip("Set TEST_POSTGRES_PASSWORD for an isolated migrated database")
    settings = Settings(
        _env_file=None,
        postgres_host=os.environ.get("TEST_POSTGRES_HOST", "localhost"),
        postgres_port=int(os.environ.get("TEST_POSTGRES_PORT", "5432")),
        postgres_db=os.environ.get("TEST_POSTGRES_DB", "switch"),
        postgres_user=os.environ.get("TEST_POSTGRES_USER", "switch"),
        postgres_password=password,
    )
    with connect(settings) as connection:
        yield settings, connection


@pytest.fixture
def event_factory():
    source = "test." + uuid4().hex

    def create(version=1, price="100.20"):
        values = example(source=source, source_version=version,
                         event_type="insert" if version == 1 else "update",
                         event_id=stable_event_id(source, "order:123", version))
        values["payload"]["total_price"] = price
        return EventEnvelope(**values)

    return create


def deliver(connection, event, offset=0):
    return materialize(connection, Delivery("test.orders", 0, offset, event.partition_key.encode(), event.model_dump_json().encode()))


def test_replay_converges_and_keeps_every_delivery(database, event_factory):
    _, connection = database
    first, newer, older = event_factory(1), event_factory(3), event_factory(2)
    assert deliver(connection, first) == "accepted"
    assert deliver(connection, newer, 1) == "accepted"
    assert deliver(connection, older, 2) == "stale"
    before = connection.execute("SELECT * FROM orders_current WHERE source = %s", (first.source,)).fetchone()
    connection.commit()
    for offset, event in enumerate((first, newer, older)):
        assert deliver(connection, event, offset) == "duplicate"
    after = connection.execute("SELECT * FROM orders_current WHERE source = %s", (first.source,)).fetchone()
    assert before == after
    assert after["source_version"] == 3
    assert connection.execute("SELECT count(*) AS n FROM event_deliveries WHERE source = %s", (first.source,)).fetchone()["n"] == 6
    assert connection.execute("SELECT count(*) AS n FROM processed_events WHERE source = %s", (first.source,)).fetchone()["n"] == 3


def test_same_version_content_conflict_is_rejected(database, event_factory):
    _, connection = database
    event = event_factory()
    assert deliver(connection, event) == "accepted"
    assert deliver(connection, event_factory(price="999.99"), 1) == "rejected"
    row = connection.execute("""SELECT reason_code FROM event_rejections
        JOIN event_deliveries USING (delivery_id) WHERE source = %s""", (event.source,)).fetchone()
    assert row["reason_code"] == "source_version_conflict"
    assert connection.execute("SELECT total_price FROM orders_current WHERE source = %s", (event.source,)).fetchone()["total_price"] == event.payload.total_price


@pytest.mark.parametrize("raw,key", [(b"not-json", None), (b"\xff", None), (None, None)])
def test_invalid_deliveries_are_durable(database, raw, key):
    _, connection = database
    topic = "test." + uuid4().hex
    assert materialize(connection, Delivery(topic, 0, 0, key, raw)) == "rejected"
    row = connection.execute("""SELECT raw_message, reason_code FROM event_deliveries
        JOIN event_rejections USING (delivery_id) WHERE topic = %s""", (topic,)).fetchone()
    assert row["raw_message"] == raw
    assert row["reason_code"]


def test_append_only_audit_is_enforced(database, event_factory):
    _, connection = database
    event = event_factory()
    deliver(connection, event)
    with pytest.raises(psycopg.errors.RaiseException):
        with connection.transaction():
            connection.execute("DELETE FROM event_deliveries WHERE source = %s", (event.source,))


def test_api_inspects_deliveries_and_current_state(database, event_factory, monkeypatch):
    import app.api.main as api

    settings, connection = database
    event = event_factory()
    deliver(connection, event)
    monkeypatch.setattr(api, "get_settings", lambda: settings)
    with TestClient(api.app) as client:
        assert client.get("/health/ready").status_code == 200
        result = client.get("/events", params={"source": event.source, "limit": 1}).json()
        assert result["items"][0]["event_id"] == event.event_id
        assert result["next_after_id"] is None
        entity = client.get("/entities/order:123", params={"source": event.source}).json()
        assert entity["current"]["source_version"] == 1
        assert entity["current"]["total_price"] == "100.20"
        assert len(entity["history"]) == 1
        assert client.get("/stats").status_code == 200
        assert client.get("/events", params={"since": "2026-01-01T00:00:00"}).status_code == 422


def test_live_broker_and_consumer_converge(database, event_factory):
    from app.transport.producer import EventProducer

    bootstrap = os.environ.get("TEST_KAFKA_BOOTSTRAP_SERVERS")
    topic = os.environ.get("TEST_KAFKA_TOPIC")
    if not bootstrap or not topic:
        pytest.skip("Set TEST_KAFKA_BOOTSTRAP_SERVERS and isolated TEST_KAFKA_TOPIC with a running test consumer")
    settings, connection = database
    settings = settings.model_copy(update={"kafka_bootstrap_servers": bootstrap, "kafka_topic": topic})
    producer = EventProducer(settings)
    first, newer, older = event_factory(1), event_factory(3, "300.00"), event_factory(2)
    for event in (first, newer, older, newer):
        producer.publish(event)
    deadline = time.monotonic() + 20
    while True:
        count = connection.execute(
            "SELECT count(*) AS n FROM event_deliveries WHERE source = %s", (first.source,)
        ).fetchone()["n"]
        connection.commit()
        if count == 4 or time.monotonic() > deadline:
            break
        time.sleep(0.2)
    assert count == 4
    counts = connection.execute("""SELECT disposition, count(*) AS n FROM event_deliveries
        WHERE source = %s GROUP BY disposition""", (first.source,)).fetchall()
    assert {row["disposition"]: row["n"] for row in counts} == {"accepted": 2, "stale": 1, "duplicate": 1}
    current = connection.execute("SELECT * FROM orders_current WHERE source = %s", (first.source,)).fetchone()
    assert current["source_version"] == 3
    assert current["total_price"] == newer.payload.total_price

@pytest.mark.parametrize("mutation,reason", [
    ("negative_price", "business_quality_violation"),
    ("missing_customer", "business_quality_violation"),
    ("missing_version", "invalid_envelope"),
    ("invalid_json", "invalid_json"),
    ("wrong_key", "partition_key_mismatch"),
])
def test_quality_checks_preserve_source_bytes_and_metadata(database, event_factory, mutation, reason):
    import json

    settings, connection = database
    event = event_factory()
    values = event.model_dump(mode="json")
    key = event.partition_key.encode()
    if mutation == "negative_price":
        values["payload"]["total_price"] = "-0.01"
    elif mutation == "missing_customer":
        del values["payload"]["customer_name"]
    elif mutation == "missing_version":
        del values["source_version"]
    elif mutation == "wrong_key":
        key = b"wrong"
    raw = b"{broken" if mutation == "invalid_json" else json.dumps(values).encode()
    topic = "test." + uuid4().hex
    assert materialize(connection, Delivery(topic, 2, 47, key, raw)) == "rejected"
    # Observe through a separate connection to prove durability, not just
    # visibility of uncommitted writes on the materializer connection.
    with connect(settings) as observer:
        row = observer.execute("""SELECT * FROM event_deliveries
            JOIN event_rejections USING (delivery_id) WHERE topic = %s""", (topic,)).fetchone()
        assert row["raw_message"] == raw
        assert row["message_key"] == key
        assert (row["partition_id"], row["broker_offset"]) == (2, 47)
        assert row["processed_at"] is not None
        assert row["reason_code"] == reason
        assert observer.execute("SELECT count(*) AS n FROM orders_current WHERE source = %s", (event.source,)).fetchone()["n"] == 0


def test_failure_after_upsert_rolls_back_log_deduplication_and_state(database, event_factory):
    _, connection = database
    first, newer = event_factory(), event_factory(2, "200.00")
    deliver(connection, first)

    class FailAfterUpsert:
        def __getattr__(self, name):
            return getattr(connection, name)

        def execute(self, query, params=None):
            result = connection.execute(query, params)
            if "INSERT INTO orders_current" in query:
                raise RuntimeError("simulated failure after real PostgreSQL upsert")
            return result

    with pytest.raises(RuntimeError, match="simulated failure"):
        deliver(FailAfterUpsert(), newer, 1)
    assert connection.execute("SELECT source_version FROM orders_current WHERE source = %s", (first.source,)).fetchone()["source_version"] == 1
    assert connection.execute("SELECT count(*) AS n FROM event_deliveries WHERE source = %s", (first.source,)).fetchone()["n"] == 1
    assert connection.execute("SELECT count(*) AS n FROM processed_events WHERE source = %s", (first.source,)).fetchone()["n"] == 1
    connection.commit()
    assert deliver(connection, newer, 1) == "accepted"


def test_materializer_refuses_uncommitted_outer_transaction(database, event_factory):
    _, connection = database
    with connection.transaction():
        with pytest.raises(RuntimeError, match="idle connection"):
            deliver(connection, event_factory())


def test_offset_failure_redelivery_is_duplicate_after_durable_commit(database, event_factory):
    from unittest.mock import Mock
    from app.consumer.main import process_message

    settings, connection = database
    event = event_factory()
    message = Mock()
    message.topic.return_value = "test.orders"
    message.partition.return_value = 0
    message.offset.return_value = 91
    message.key.return_value = event.partition_key.encode()
    message.value.return_value = event.model_dump_json().encode()
    consumer = Mock()

    def failed_ack(**kwargs):
        with connect(settings) as observer:
            assert observer.execute("SELECT source_version FROM orders_current WHERE source = %s", (event.source,)).fetchone()["source_version"] == 1
        raise RuntimeError("offset acknowledgement failed")

    consumer.commit.side_effect = failed_ack
    with pytest.raises(RuntimeError, match="acknowledgement failed"):
        process_message(consumer, connection, message)
    consumer.commit.side_effect = None
    assert process_message(consumer, connection, message) == "duplicate"
    consumer.commit.assert_called_with(message=message, asynchronous=False)
    rows = connection.execute("SELECT delivery_id, disposition FROM event_deliveries WHERE source = %s ORDER BY delivery_id", (event.source,)).fetchall()
    assert [row["disposition"] for row in rows] == ["accepted", "duplicate"]
    assert rows[0]["delivery_id"] != rows[1]["delivery_id"]
    assert connection.execute("SELECT count(*) AS n FROM event_rejections JOIN event_deliveries USING (delivery_id) WHERE source = %s", (event.source,)).fetchone()["n"] == 0


def test_database_failure_does_not_acknowledge_offset(database, event_factory):
    from unittest.mock import Mock
    from app.consumer.main import process_message

    _, connection = database
    event = event_factory()
    message = Mock()
    message.topic.return_value = "test.orders"
    message.partition.return_value = 0
    message.offset.return_value = 0
    message.key.return_value = event.partition_key.encode()
    message.value.return_value = event.model_dump_json().encode()
    consumer = Mock()
    # PostgreSQL rejects the write; the consumer must leave the offset alone.
    connection.execute("SET default_transaction_read_only = on")
    connection.commit()
    try:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            process_message(consumer, connection, message)
        consumer.commit.assert_not_called()
    finally:
        connection.execute("SET default_transaction_read_only = off")
        connection.commit()

@pytest.mark.parametrize("versions", [(1, 2, 3), (3, 2, 1), (2, 1, 3), (1, 3, 2)])
def test_delivery_order_converges_to_latest_business_state(database, event_factory, versions):
    _, connection = database
    events = {version: event_factory(version, f"{version}00.00") for version in (1, 2, 3)}
    for offset, version in enumerate(versions):
        deliver(connection, events[version], offset)
    current = connection.execute("SELECT * FROM orders_current WHERE source = %s", (events[3].source,)).fetchone()
    assert current["source_version"] == 3
    assert current["payload"] == events[3].payload.model_dump(mode="json")
    assert current["last_event_id"] == events[3].event_id
    connection.commit()
    # Replay from zero on the same projection is a no-op for business state.
    for offset, version in enumerate(versions):
        assert deliver(connection, events[version], offset) == "duplicate"
    assert connection.execute("SELECT * FROM orders_current WHERE source = %s", (events[3].source,)).fetchone() == current


def test_duplicate_with_new_capture_metadata_is_not_quarantined(database, event_factory):
    _, connection = database
    first, recaptured = event_factory(), event_factory()
    assert first.batch_id != recaptured.batch_id
    assert deliver(connection, first) == "accepted"
    assert deliver(connection, recaptured, 1) == "duplicate"
    assert connection.execute("SELECT count(*) AS n FROM event_rejections JOIN event_deliveries USING (delivery_id) WHERE source = %s", (first.source,)).fetchone()["n"] == 0
