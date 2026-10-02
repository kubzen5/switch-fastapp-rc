from contextlib import contextmanager
import hashlib

from app.db.connection import connect


@contextmanager
def source_lock(settings):
    """Session lock, independent of batch transactions. All writers must cooperate."""
    key = int.from_bytes(hashlib.sha256(settings.source_name.encode()).digest()[:8], 'big', signed=True)
    with connect(settings) as conn:
        conn.autocommit = True
        conn.execute('SET statement_timeout = 0')
        conn.execute('SELECT pg_advisory_lock(%s)', (key,))
        try:
            conn.execute('INSERT INTO source_write_fences(source) VALUES (%s) ON CONFLICT DO NOTHING', (settings.source_name,))
            if conn.execute('SELECT blocked FROM source_write_fences WHERE source=%s', (settings.source_name,)).fetchone()['blocked']:
                raise RuntimeError('Source fenced: resolve interrupted writer before syncing; see docs/snowflake.md')
            yield conn
        finally:
            conn.execute('SELECT pg_advisory_unlock(%s)', (key,))
