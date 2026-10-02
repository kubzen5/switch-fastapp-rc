"""Optional demo exclusion, shared by the normal and replay consumer."""
from contextlib import contextmanager
import hashlib

from app.db.connection import connect


@contextmanager
def consumer_lock(settings):
    if not settings.consumer_exclusive:
        yield
        return
    # Deliberately independent of group.id: a replay group cannot bypass it.
    key = int.from_bytes(hashlib.sha256(
        ('consumer:' + settings.kafka_topic).encode()
    ).digest()[:8], 'big', signed=True)
    with connect(settings) as connection:
        connection.autocommit = True
        if not connection.execute('SELECT pg_try_advisory_lock(%s) AS locked', (key,)).fetchone()['locked']:
            raise RuntimeError('Another demo consumer owns this topic; stop it before replay')
        try:
            yield
        finally:
            connection.execute('SELECT pg_advisory_unlock(%s)', (key,))
