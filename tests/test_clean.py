from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
import pytest

from app.db.clean import clean
from tests.test_materializer import database, event_factory, deliver

pytestmark = pytest.mark.integration


@pytest.fixture
def isolated(database):
    _, conn = database
    schema = 'clean_' + uuid4().hex
    conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
    for path in sorted(Path('app/db/migrations').glob('*.sql')):
        conn.execute(path.read_text())
    conn.commit()
    try:
        yield conn
    finally:
        conn.rollback()
        conn.execute('SET search_path TO public')
        conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
        conn.commit()


def test_inspects_before_clearing_and_keeps_audit_protection(isolated, event_factory):
    event = event_factory()
    deliver(isolated, event)
    inspected = clean(isolated)
    assert inspected['before']['event_deliveries'] == 1
    assert not inspected['cleared']
    result = clean(isolated, execute=True)
    assert result['cleared'] and set(result['after'].values()) == {0}
    deliver(isolated, event)
    row = isolated.execute('SELECT delivery_id FROM event_deliveries').fetchone()
    assert row['delivery_id'] > 1
    isolated.commit()
    with pytest.raises(psycopg.errors.RaiseException):
        with isolated.transaction():
            isolated.execute('TRUNCATE event_deliveries, processed_events, orders_current, event_rejections')


def test_empty_tables_are_noop(isolated):
    result = clean(isolated, execute=True, scope='pipeline')
    assert not result['cleared'] and set(result['before'].values()) == {0}


def test_missing_schema_tables_are_reported(database):
    _, conn = database
    schema = 'empty_' + uuid4().hex
    conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
    conn.commit()
    try:
        result = clean(conn, execute=True)
        assert result['before'] == {} and len(result['missing_tables']) == 4
        assert not result['cleared']
    finally:
        conn.execute('SET search_path TO public')
        conn.execute(sql.SQL('DROP SCHEMA {}').format(sql.Identifier(schema)))
        conn.commit()


def test_foreign_key_failure_rolls_back_trigger_changes(isolated, event_factory):
    event = event_factory()
    deliver(isolated, event)
    isolated.execute('CREATE TABLE external_reference (id bigint REFERENCES event_deliveries(delivery_id))')
    isolated.commit()
    with pytest.raises(psycopg.errors.FeatureNotSupported):
        clean(isolated, execute=True)
    assert isolated.execute('SELECT count(*) AS n FROM event_deliveries').fetchone()['n'] == 1
    isolated.commit()
    with pytest.raises(psycopg.errors.RaiseException):
        with isolated.transaction():
            isolated.execute('DELETE FROM event_deliveries')


def test_pipeline_refuses_active_fence(isolated):
    isolated.execute("INSERT INTO source_write_fences(source,blocked) VALUES ('test',true)")
    isolated.execute("INSERT INTO sync_state(source) VALUES ('test')")
    isolated.commit()
    with pytest.raises(RuntimeError, match='fence'):
        clean(isolated, execute=True, scope='pipeline')
    assert isolated.execute('SELECT count(*) AS n FROM sync_state').fetchone()['n'] == 1
    isolated.commit()
    isolated.execute('UPDATE source_write_fences SET blocked=false')
    isolated.commit()
    assert clean(isolated, execute=True, scope='pipeline')['cleared']
    assert isolated.execute('SELECT count(*) AS n FROM source_write_fences').fetchone()['n'] == 1
