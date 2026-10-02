"""Behavior tests with in-memory journal/checkpoint; NOT integration tests."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from uuid import uuid4
import pytest

from app.adapter.sync import run_sync
from app.domain.cursor import SyncCursor

T = datetime(2026, 1, 1, tzinfo=timezone.utc)


def row(key, stamp=T, version=1, price='10.00'):
    return dict(order_key=key, customer_key=1, customer_name='Sample', order_status='O', total_price=Decimal(price), order_date=T.date(), source_updated_at=stamp, source_version=version, event_type='insert' if version == 1 else 'update')


def cursor(record):
    return SyncCursor(source_updated_at=record['source_updated_at'], order_key=record['order_key'])


class Journal:
    def __init__(self, rows):
        self.rows = rows

    def high_watermark(self):
        return cursor(self.rows[-1]) if self.rows else None

    def read_batch(self, after, high, limit):
        return [r for r in self.rows if (after is None or cursor(r).follows(after)) and not cursor(r).follows(high)][:limit]


class Store:
    def __init__(self):
        self.checkpoint = None
        self.quarantine = []
        self.commits = []
        self.fail_commit = False
        self.runs = []

    def load(self, source):
        return self.checkpoint

    def start_run(self, source, after, high):
        self.runs.append('incremental' if after else 'initial')
        return uuid4()

    def start_batch(self, *args):
        return uuid4()

    def commit_batch(self, source, batch, end, events, rejected):
        if self.fail_commit:
            raise RuntimeError('Postgres unavailable')
        self.quarantine.extend(deepcopy(rejected))
        self.commits.append([e.event_id for e in events])
        self.checkpoint = end

    def finish_run(self, *args):
        pass


class Publisher:
    def __init__(self, fail_at=None):
        self.events = []
        self.fail_at = fail_at

    def publish(self, event):
        if len(self.events) == self.fail_at:
            raise RuntimeError('ACK unavailable')
        self.events.append(event)


def sync(journal, store, publisher, size=2):
    return run_sync(journal, store, publisher, 'snowflake.orders', size)


def test_ties_boundaries_initial_incremental_repeated_update_and_no_reemit():
    journal, store, publisher = Journal([row(i) for i in range(1, 6)]), Store(), Publisher()
    assert sync(journal, store, publisher) == 5
    assert sync(journal, store, Publisher()) == 0
    journal.rows += [row(1, T+timedelta(microseconds=1), 2), row(6, T+timedelta(microseconds=1)), row(1, T+timedelta(microseconds=2), 3)]
    assert sync(journal, store, publisher, 1) == 3
    assert [e.source_version for e in publisher.events if e.entity_key == 'order:1'] == [1, 2, 3]
    assert store.runs == ['initial', 'incremental']
    assert sync(journal, store, Publisher()) == 0


@pytest.mark.parametrize('failure', ['publish', 'checkpoint'])
def test_restart_partial_batch_replays_stable_ids_without_loss(failure):
    journal, store, publisher = Journal([row(i) for i in range(1, 6)]), Store(), Publisher()
    # First completed batch survives restart.
    assert sync(Journal(journal.rows[:2]), store, publisher) == 2
    broken = Publisher(fail_at=1 if failure == 'publish' else None)
    store.fail_commit = failure == 'checkpoint'
    with pytest.raises(RuntimeError):
        sync(journal, store, broken)
    assert store.checkpoint == cursor(row(2))
    store.fail_commit = False
    restarted = Publisher()
    assert sync(journal, store, restarted) == 3
    assert broken.events[0].event_id == restarted.events[0].event_id
    assert {e.entity_key for e in publisher.events + restarted.events} == {f'order:{i}' for i in range(1, 6)}
    assert sync(journal, store, Publisher()) == 0


def test_quarantine_is_durable_before_checkpoint_and_failure_retries():
    journal, store, publisher = Journal([row(1, price='-1'), row(2)]), Store(), Publisher()
    store.fail_commit = True
    with pytest.raises(RuntimeError):
        sync(journal, store, publisher)
    assert store.checkpoint is None and not store.quarantine
    store.fail_commit = False
    assert sync(journal, store, Publisher()) == 2
    assert store.quarantine[0][0]['order_key'] == 1
    assert store.checkpoint == cursor(row(2))


def test_duplicate_cursor_fails_without_checkpoint():
    store = Store()
    with pytest.raises(RuntimeError, match='cursor order'):
        sync(Journal([row(1), row(1)]), store, Publisher())
    assert store.checkpoint is None


def test_empty_source_does_not_initialize_checkpoint():
    store = Store()
    assert sync(Journal([]), store, Publisher()) == 0
    assert store.checkpoint is None


def test_run_freezes_high_watermark_then_next_run_captures_new_version():
    journal, store = Journal([row(1), row(2)]), Store()

    class ConcurrentPublisher(Publisher):
        def publish(self, event):
            super().publish(event)
            if len(self.events) == 1:
                journal.rows.append(row(1, T + timedelta(microseconds=1), 2))

    publisher = ConcurrentPublisher()
    assert sync(journal, store, publisher, 1) == 2
    assert store.checkpoint == cursor(row(2))
    assert sync(journal, store, Publisher()) == 1


def test_quarantine_only_batch_moves_checkpoint_without_publishing():
    store, publisher = Store(), Publisher()
    assert sync(Journal([row(1, price='-1')]), store, publisher) == 1
    assert len(store.quarantine) == 1
    assert not publisher.events


@pytest.mark.parametrize('records', [[], [row(1)]])
def test_journal_reset_fails_closed(records):
    store = Store()
    store.checkpoint = cursor(row(2))
    with pytest.raises(RuntimeError, match='regressed'):
        sync(Journal(records), store, Publisher())
    assert store.checkpoint == cursor(row(2))
