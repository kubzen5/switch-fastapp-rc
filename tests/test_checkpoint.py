"""Optional real PostgreSQL checkpoint tests; no Snowflake integration implied."""
from uuid import uuid4
import pytest
from app.adapter.checkpoint import PostgresCheckpoint
from app.adapter.sync import event_from_row
from tests.test_materializer import database
from tests.test_sync import row, cursor

pytestmark = pytest.mark.integration


def test_checkpoint_quarantine_and_versions_survive_new_connection(database):
    settings, conn = database
    conn.autocommit = True
    source = 'checkpoint.' + uuid4().hex
    store = PostgresCheckpoint(conn)
    first, bad = row(1), row(2, price='-1')
    high = cursor(bad)
    run = store.start_run(source, None, high)
    batch = store.start_batch(run, 1, None, high, 2)
    event = event_from_row(first, source, batch, run, 1)
    store.commit_batch(source, batch, high, [event], [(bad, 'negative amount')])
    store.finish_run(run, 'completed')
    from app.db.connection import connect
    with connect(settings) as restarted:
        assert PostgresCheckpoint(restarted).load(source) == high
        assert restarted.execute('SELECT count(*) AS n FROM source_quarantine WHERE source=%s', (source,)).fetchone()['n'] == 1
        assert restarted.execute('SELECT source_version FROM source_entity_versions WHERE source=%s', (source,)).fetchone()['source_version'] == 1


def test_failed_atomic_checkpoint_rolls_back_quarantine(database):
    _, conn = database
    conn.autocommit = True
    source = 'rollback.' + uuid4().hex
    store = PostgresCheckpoint(conn)
    bad = row(1, price='-1')
    high = cursor(bad)
    run = store.start_run(source, None, high)
    batch = store.start_batch(run, 1, None, high, 1)
    # Inject DB failure after quarantine INSERT, before state update.
    with conn.transaction():
        conn.execute("""CREATE FUNCTION pg_temp.reject_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'injected checkpoint failure'; END; $$""")
        conn.execute('CREATE TRIGGER test_checkpoint_failure BEFORE INSERT ON sync_state FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_checkpoint()')
    try:
        with pytest.raises(Exception, match='injected checkpoint failure'):
            store.commit_batch(source, batch, high, [], [(bad, 'invalid')])
        assert store.load(source) is None
        assert conn.execute('SELECT count(*) AS n FROM source_quarantine WHERE source=%s', (source,)).fetchone()['n'] == 0
        assert conn.execute('SELECT status FROM sync_batches WHERE batch_id=%s', (batch,)).fetchone()['status'] == 'running'
    finally:
        conn.execute('DROP TRIGGER test_checkpoint_failure ON sync_state')


def test_checkpoint_refuses_savepoint_in_outer_transaction(database):
    _, conn = database
    with conn.transaction():
        with pytest.raises(RuntimeError, match='idle connection'):
            PostgresCheckpoint(conn).commit_batch('nested', uuid4(), cursor(row(1)), [], [])


def test_checkpoint_entity_version_cannot_regress(database):
    from datetime import timedelta
    from tests.test_sync import T
    _, conn = database
    conn.autocommit = True
    source = 'version.' + uuid4().hex
    store = PostgresCheckpoint(conn)
    for version in (3, 2):
        record = row(1, T + timedelta(seconds=4-version), version)
        end = cursor(record)
        run = store.start_run(source, store.load(source), end)
        batch = store.start_batch(run, 1, store.load(source), end, 1)
        event = event_from_row(record, source, batch, run, 1)
        store.commit_batch(source, batch, end, [event], [])
        store.finish_run(run, 'completed')
    assert conn.execute('SELECT source_version FROM source_entity_versions WHERE source=%s', (source,)).fetchone()['source_version'] == 3
