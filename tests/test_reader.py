"""Execute the paging SQL with a real window engine, without Snowflake credentials."""
import sqlite3

import pytest

from app.adapter.snowflake_client import SnowflakeReader
from tests.test_sync import cursor, row


class SqlJournal(SnowflakeReader):
    def __init__(self, records):
        self.table = 'journal'
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('CREATE TABLE journal(source_updated_at TEXT, order_key INTEGER)')
        self.conn.executemany('INSERT INTO journal VALUES (?,?)',
                             [(r['source_updated_at'].isoformat(), r['order_key']) for r in records])

    def query(self, sql, params=()):
        return [dict(r) for r in self.conn.execute(sql.replace('%s', '?'),
                tuple(p.isoformat() if hasattr(p, 'isoformat') else p for p in params))]


def test_duplicate_cursor_straddling_limit_is_rejected_before_checkpoint():
    reader = SqlJournal([row(9), row(10), row(10), row(11)])
    with pytest.raises(RuntimeError, match='duplicate cursors'):
        reader.read_batch(cursor(row(9)), cursor(row(11)), 1)


def test_sql_keyset_pages_every_tied_timestamp_record_and_respects_high():
    reader = SqlJournal([row(9), row(10), row(11), row(12)])
    first = reader.read_batch(None, cursor(row(11)), 2)
    second = reader.read_batch(cursor(row(10)), cursor(row(11)), 2)
    assert [r['order_key'] for r in first + second] == [9, 10, 11]
    assert all('cursor_multiplicity' not in r for r in first + second)
