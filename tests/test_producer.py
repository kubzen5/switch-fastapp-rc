"""Fault injection with a fake Kafka client; no live broker involved."""
import json
import logging
import threading
from uuid import uuid4

import pytest

from app.adapter.sync import event_from_row, run_sync
from app.config import Settings
from app.logging import JsonFormatter
from app.transport.producer import EventProducer, PublicationFailed, PublicationStopped
from tests.test_sync import Journal, Store, row, cursor


class Message:
    def partition(self):
        return 1

    def offset(self):
        return 42


class Client:
    def __init__(self, outcomes):
        self.outcomes = iter(outcomes)
        self.sent = []
        self.pending = None
        self.flush_calls = []
        self.purged = False
        self.remaining = 0

    def produce(self, topic, **kwargs):
        outcome = next(self.outcomes)
        if outcome == 'full':
            raise BufferError()
        self.sent.append((topic, kwargs['key'], kwargs['value']))
        self.pending = (kwargs['on_delivery'], outcome)

    def poll(self, timeout):
        if self.pending:
            callback, outcome = self.pending
            self.pending = None
            callback(outcome, Message())

    def flush(self, timeout):
        self.flush_calls.append(timeout)
        return self.remaining

    def purge(self, **kwargs):
        self.purged = True


def settings(**kwargs):
    return Settings(_env_file=None, postgres_password='test', **kwargs)


def event():
    return event_from_row(row(1), 'snowflake.orders', uuid4(), uuid4(), 1)


def test_ack_retries_identical_bytes_partition_and_json_logs(monkeypatch, caplog):
    caps = []
    monkeypatch.setattr('app.transport.producer.random.uniform', lambda low, high: caps.append(high) or 0)
    client = Client([True, True, True, None])
    publisher = EventProducer(settings(kafka_retry_base_seconds=2, kafka_retry_max_seconds=5), producer=client)
    original = event()
    with caplog.at_level(logging.INFO):
        publisher.publish(original)
    assert caps == [2, 4, 5]
    assert len(client.sent) == 4 and len(set(client.sent)) == 1
    assert client.sent[0][1] == b'order:1'
    records = [json.loads(JsonFormatter().format(r)) for r in caplog.records]
    assert [r['message'] for r in records] == ['publication_retry'] * 3 + ['event_published']
    assert all(r['event_id'] == original.event_id and r['batch_id'] == str(original.batch_id) and r['source'] == original.source for r in records)
    assert records[-1]['offset'] == 42


@pytest.mark.parametrize('prefix', [0, 1])
def test_broker_down_before_or_mid_batch_does_not_checkpoint(prefix, monkeypatch):
    monkeypatch.setattr('app.transport.producer.random.uniform', lambda *_: 0)
    client = Client([None] * prefix + [True] * 4)
    store = Store()
    with EventProducer(settings(), producer=client) as publisher:
        with pytest.raises(PublicationFailed, match='exhausted'):
            run_sync(Journal([row(1), row(2)]), store, publisher, 'snowflake.orders', 2)
    assert store.checkpoint is None and not store.commits
    assert len(client.sent) == prefix + 4
    assert client.flush_calls == [5]


def test_written_but_ack_lost_replays_same_id(monkeypatch):
    monkeypatch.setattr('app.transport.producer.random.uniform', lambda *_: 0)
    # Each produce models a durable broker write, even when callback reports timeout.
    client = Client([True] * 4)
    store = Store()
    with pytest.raises(PublicationFailed):
        run_sync(Journal([row(1)]), store, EventProducer(settings(), producer=client), 'snowflake.orders', 2)
    assert store.checkpoint is None
    restarted = Client([None])
    run_sync(Journal([row(1)]), store, EventProducer(settings(), producer=restarted), 'snowflake.orders', 2)
    ids = [json.loads(item[2])['event_id'] for item in client.sent + restarted.sent]
    assert len(set(ids)) == 1 and len(ids) == 5
    assert store.checkpoint == cursor(row(1))


def test_crash_after_ack_before_checkpoint():
    class CrashStore(Store):
        def commit_batch(self, *args):
            raise SystemExit('simulated process death')

    store, client = CrashStore(), Client([None, None])
    with pytest.raises(SystemExit):
        run_sync(Journal([row(1), row(2)]), store, EventProducer(settings(), producer=client), 'snowflake.orders', 2)
    assert store.checkpoint is None
    restarted, replay = Store(), Client([None, None])
    run_sync(Journal([row(1), row(2)]), restarted, EventProducer(settings(), producer=replay), 'snowflake.orders', 2)
    assert [json.loads(s[2])['event_id'] for s in client.sent] == [json.loads(s[2])['event_id'] for s in replay.sent]


def test_queue_full_retry(monkeypatch):
    monkeypatch.setattr('app.transport.producer.random.uniform', lambda *_: 0)
    client = Client(['full', None])
    EventProducer(settings(), producer=client).publish(event())
    assert len(client.sent) == 1


def test_missing_callback_has_bounded_wait_and_no_overlapping_retry(monkeypatch):
    ticks = iter([0, 0, 0, 12])
    monkeypatch.setattr('app.transport.producer.time.monotonic', lambda: next(ticks))
    client = Client([None])
    monkeypatch.setattr(client, 'poll', lambda _: None)
    client.remaining = 1
    with EventProducer(settings(), producer=client) as publisher:
        with pytest.raises(PublicationFailed, match='callback timeout'):
            publisher.publish(event())
    assert len(client.sent) == 1 and client.purged and client.flush_calls == [5]
    with pytest.raises(PublicationFailed, match='closed'):
        publisher.publish(event())


def test_shutdown_during_inflight_drains_ack_but_retains_batch():
    stopped, store = threading.Event(), Store()
    client = Client([None])
    original_poll = client.poll

    def poll(timeout):
        stopped.set()
        original_poll(timeout)

    client.poll = poll

    def pulse():
        if stopped.is_set():
            raise PublicationStopped()

    with EventProducer(settings(), stopped, producer=client) as publisher:
        with pytest.raises(PublicationStopped):
            run_sync(Journal([row(1)]), store, publisher, 'snowflake.orders', 2, pulse)
    assert len(client.sent) == 1 and store.checkpoint is None
    assert client.flush_calls == [5]


def test_shutdown_interrupts_backoff_and_prevents_new_enqueues():
    class StopOnWait:
        def is_set(self):
            return False

        def wait(self, delay):
            return True

    client = Client([True])
    with pytest.raises(PublicationStopped, match='retry'):
        EventProducer(settings(), StopOnWait(), producer=client).publish(event())
    assert len(client.sent) == 1


def test_stop_before_publication_does_not_enqueue():
    stopped = threading.Event()
    stopped.set()
    client = Client([])
    with EventProducer(settings(), stopped, producer=client) as publisher:
        with pytest.raises(PublicationStopped):
            publisher.publish(event())
    publisher.close()
    assert not client.sent and client.flush_calls == [5]


def test_fatal_callback_does_not_retry():
    class Fatal:
        def fatal(self):
            return True

    client = Client([Fatal()])
    with pytest.raises(PublicationFailed):
        EventProducer(settings(), producer=client).publish(event())
    assert len(client.sent) == 1
