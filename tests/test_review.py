from uuid import uuid4

import pytest

from app.consumer.coordination import consumer_lock
from app.db.connection import connect
from app.db.state import state_fingerprint
from tests.test_materializer import database, event_factory, deliver

pytestmark = pytest.mark.integration


def test_demo_lock_excludes_another_group_and_releases_after_exit(database):
    settings, _ = database
    settings = settings.model_copy(update={'consumer_exclusive': True, 'kafka_topic': 'lock.' + uuid4().hex})
    another_group = settings.model_copy(update={'kafka_group_id': 'other-group'})
    with consumer_lock(settings):
        with pytest.raises(RuntimeError, match='Another demo consumer'):
            with consumer_lock(another_group):
                pytest.fail('Concurrent demo consumer admitted')
    with consumer_lock(another_group):
        pass


def test_hash_covers_payload_version_and_ignores_processing_metadata(database, event_factory):
    settings, conn = database
    first = event_factory()
    deliver(conn, first)
    before = state_fingerprint(conn)
    conn.commit()
    deliver(conn, first)
    assert state_fingerprint(conn) == before
    conn.commit()
    conn.execute('UPDATE orders_current SET updated_at=now(),last_captured_at=now() WHERE source=%s', (first.source,))
    conn.commit()
    assert state_fingerprint(conn) == before
    conn.commit()
    deliver(conn, event_factory(2, '100.20'))
    newer = state_fingerprint(conn)
    assert newer['entities'] == before['entities'] and newer['sha256'] != before['sha256']
    conn.commit()
    # Same count and version, different payload: fingerprint must still change.
    conn.execute("UPDATE orders_current SET payload=jsonb_set(payload,'{customer_name}','\"Changed\"'::jsonb) WHERE source=%s", (first.source,))
    conn.commit()
    changed = state_fingerprint(conn)
    assert changed['entities'] == newer['entities'] and changed['sha256'] != newer['sha256']
