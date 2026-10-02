from datetime import timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

import app.api.main as api
from app.adapter.checkpoint import PostgresCheckpoint
from app.consumer.materializer import Delivery, materialize
from app.domain.cursor import SyncCursor
from tests.test_materializer import database, event_factory, deliver


@pytest.fixture
def client(database, monkeypatch):
    settings, _ = database
    monkeypatch.setattr(api, 'get_settings', lambda: settings)
    with TestClient(api.app) as client:
        yield client


@pytest.mark.parametrize('params', [
    {'limit': 0}, {'limit': 501}, {'after_id': -1}, {'after_id': 9223372036854775808}, {'event_type': 'delete'},
    {'entity_key': ''}, {'source': ''}, {'since': 'bad'},
    {'since': '2026-01-01T00:00:00'},
    {'since': '2026-02-01T00:00:00Z', 'until': '2026-01-01T00:00:00Z'},
])
def test_parameter_validation_without_database(params):
    # Validation must fail before attempting a connection.
    client = TestClient(api.app)
    assert client.get('/events', params=params).status_code == 422


@pytest.mark.integration
def test_filters_and_keyset_pages(client, database, event_factory):
    _, connection = database
    first, newer, older = event_factory(1), event_factory(3), event_factory(2)
    for event in (first, newer, older, newer):
        deliver(connection, event)
    params = {'source': first.source, 'entity_key': first.entity_key, 'limit': 2}
    page = client.get('/events', params=params).json()
    assert len(page['items']) == 2
    cursor = page['next_after_id']
    # Appending a new delivery cannot shift the next page as offset pagination would.
    deliver(connection, first)
    following = client.get('/events', params={**params, 'after_id': cursor}).json()
    last = client.get('/events', params={**params, 'after_id': following['next_after_id']}).json()
    ids = [row['delivery_id'] for row in page['items'] + following['items'] + last['items']]
    assert ids == sorted(set(ids)) and len(ids) == 5
    assert last['next_after_id'] is None
    assert client.get('/events', params={**params, 'after_id': ids[-1]}).json()['items'] == []
    updates = client.get('/events', params={**params, 'event_type': 'update', 'limit': 500}).json()
    assert len(updates['items']) == 3
    instant = first.occurred_at.isoformat()
    assert len(client.get('/events', params={**params, 'since': instant, 'until': instant, 'limit': 500}).json()['items']) == 5
    assert client.get('/events', params={**params, 'since': (first.occurred_at + timedelta(seconds=1)).isoformat()}).json()['items'] == []
    entity = client.get(f'/entities/{first.entity_key}', params={'source': first.source, 'limit': 2}).json()
    assert entity['current']['source_version'] == 3
    assert len(entity['history']) == 2 and entity['next_after_id'] == cursor
    empty_history = client.get(f'/entities/{first.entity_key}', params={'source': first.source, 'after_id': ids[-1]})
    assert empty_history.status_code == 200 and empty_history.json()['history'] == []
    assert client.get('/entities/absent', params={'source': first.source}).status_code == 404


@pytest.mark.integration
def test_stats_match_durable_rows(client, database, event_factory):
    _, connection = database
    first, newer, older = event_factory(1), event_factory(3), event_factory(2)
    for event in (first, newer, older, first):
        deliver(connection, event)
    # A conflict shares the event ID but must not display the accepted envelope.
    deliver(connection, event_factory(1, '999.00'))
    materialize(connection, Delivery('api.test', 0, 0, None, b'bad json'))
    connection.commit()
    result = client.get('/stats').json()
    expected = connection.execute("""SELECT count(*) AS deliveries,
        count(*) FILTER (WHERE disposition IN ('accepted','stale')) AS unique_events,
        count(*) FILTER (WHERE disposition='accepted') AS accepted,
        count(*) FILTER (WHERE disposition='stale') AS stale,
        count(*) FILTER (WHERE disposition='duplicate') AS duplicates,
        count(*) FILTER (WHERE disposition='rejected') AS rejected FROM event_deliveries""").fetchone()
    assert result['counts'] == expected
    assert expected['unique_events'] == connection.execute('SELECT count(*) AS n FROM processed_events').fetchone()['n']
    assert expected['deliveries'] == expected['unique_events'] + expected['duplicates'] + expected['rejected']
    for key in expected:
        assert sum(row[key] for row in result['counts_by_type']) == expected[key]
    expected_delay = connection.execute("""SELECT count(*) AS n,
        avg(extract(epoch FROM d.processed_at-d.occurred_at)) AS avg
        FROM processed_events p JOIN event_deliveries d ON d.delivery_id=p.first_delivery_id""").fetchone()
    assert result['processing_delay']['sample_count'] == expected_delay['n']
    assert result['processing_delay']['avg_seconds'] == pytest.approx(float(expected_delay['avg']))
    assert result['processing_delay']['unit'] == 'seconds'
    rows = client.get('/events', params={'source': first.source}).json()['items']
    assert rows[-1]['disposition'] == 'rejected' and rows[-1]['envelope'] is None
    assert any(row['event_type'] is None for row in result['counts_by_type'])


@pytest.mark.integration
def test_watermark_and_completion(client, database):
    _, connection = database
    source = 'api.' + uuid4().hex
    from datetime import datetime, timezone
    cursor = SyncCursor(source_updated_at=datetime.now(timezone.utc), order_key=123)
    checkpoint = PostgresCheckpoint(connection)
    run = checkpoint.start_run(source, None, cursor)
    before = client.get('/stats').json()
    row = next(w for w in before['watermarks'] if w['source'] == source)
    assert row['last_updated_at'] is None and row['last_sync_completed_at'] is None
    batch = checkpoint.start_batch(run, 1, None, cursor, 0)
    checkpoint.commit_batch(source, batch, cursor, [], [({'source_updated_at': cursor.source_updated_at, 'order_key': 123}, 'invalid record')])
    checkpoint.finish_run(run, 'completed')
    row = next(w for w in client.get('/stats').json()['watermarks'] if w['source'] == source)
    assert row['last_order_key'] == 123 and row['last_completed_batch_id'] == str(batch)
    assert row['batch_completed_at'] and row['last_sync_completed_at']
    expected = connection.execute('SELECT count(*) AS n FROM source_quarantine').fetchone()['n']
    assert client.get('/stats').json()['adapter_rejected_records'] == expected


def test_health_failure(monkeypatch):
    def unavailable(_):
        raise RuntimeError('offline')
    monkeypatch.setattr(api, 'check_postgres', unavailable)
    client = TestClient(api.app)
    assert client.get('/health/live').json() == {'status': 'ok'}
    assert client.get('/health/ready').status_code == 503


@pytest.mark.integration
def test_empty_database(database, monkeypatch):
    """Use a fresh schema so the test is independent of earlier audit records."""
    from pathlib import Path
    from psycopg import sql
    from app.db.connection import connect

    settings, connection = database
    schema = 'api_empty_' + uuid4().hex
    connection.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    connection.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
    for path in sorted(Path('app/db/migrations').glob('*.sql')):
        connection.execute(path.read_text())
    connection.commit()

    def isolated_connect(_):
        conn = connect(settings)
        conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
        conn.commit()
        return conn

    monkeypatch.setattr(api, 'connect', isolated_connect)
    monkeypatch.setattr(api, 'get_settings', lambda: settings)
    try:
        client = TestClient(api.app)
        assert client.get('/events').json() == {'items': [], 'next_after_id': None}
        assert client.get('/entities/missing').status_code == 404
        result = client.get('/stats').json()
        assert set(result['counts'].values()) == {0}
        assert result['counts_by_type'] == [] and result['watermarks'] == []
        assert result['adapter_rejected_records'] == 0
        assert result['processing_delay']['sample_count'] == 0
        assert result['processing_delay']['avg_seconds'] is None
        assert result['processing_delay']['min_seconds'] is None
        assert result['processing_delay']['max_seconds'] is None
    finally:
        connection.execute('SET search_path TO public')
        connection.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
        connection.commit()


@pytest.mark.integration
def test_occurrence_time_is_not_capture_or_processing_time(client, database, event_factory):
    from datetime import timezone
    _, connection = database
    event = event_factory()
    event = event.model_copy(update={'captured_at': event.occurred_at + timedelta(days=1)})
    deliver(connection, event)
    params = {'source': event.source}
    # Equivalent timezone offsets match the inclusive boundary.
    instant = event.occurred_at.astimezone(timezone(timedelta(hours=2))).isoformat()
    assert len(client.get('/events', params={**params, 'since': instant, 'until': instant}).json()['items']) == 1
    captured = event.captured_at.isoformat()
    assert client.get('/events', params={**params, 'since': captured, 'until': captured}).json()['items'] == []
    materialize(connection, Delivery('api.invalid', 0, 1, None, b'{}'))
    connection.commit()
    # Unknown timestamps remain visible only when no time predicate is present.
    rows = client.get('/events', params={'since': instant, 'until': instant, 'limit': 500}).json()['items']
    assert all(row['occurred_at'] is not None for row in rows)


@pytest.mark.integration
def test_rejected_only_entity_exists_after_history_cursor(client, database, event_factory):
    _, connection = database
    event = event_factory()
    materialize(connection, Delivery('api.rejected', 0, 1, b'wrong key', event.model_dump_json().encode()))
    response = client.get(f'/entities/{event.entity_key}', params={
        'source': event.source, 'after_id': 9223372036854775807,
    })
    assert response.status_code == 200
    assert response.json() == {'current': None, 'history': [], 'next_after_id': None}
