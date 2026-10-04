"""Exercise append SQL with SQLite; no Snowflake credentials or source writes."""
from datetime import datetime, timezone
import sqlite3
from types import SimpleNamespace

import pytest

from app.adapter import demo


SETTINGS = SimpleNamespace(snowflake_database='demo', snowflake_schema='public', snowflake_table='orders')


class LocalCursor:
    def __init__(self, source):
        self.source = source
        self.cursor = source.database.cursor()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.cursor.close()

    def execute(self, sql, params=()):
        self.source.statements.append(sql)
        for original, local in (
            ('DEMO.PUBLIC.ORDERS_CHANGES', 'orders_changes'),
            ('DEMO.PUBLIC.ORDERS', 'orders'),
            ('SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS', 'sample_orders'),
            ('SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER', 'sample_customers'),
        ):
            sql = sql.replace(original, local)
        self.cursor.execute(sql.replace('%s', '?'), tuple(
            value.isoformat(timespec='microseconds') if isinstance(value, datetime) else value
            for value in params
        ))

    @property
    def description(self):
        return self.cursor.description

    def fetchall(self):
        return self.cursor.fetchall()


class LocalSource:
    def __init__(self):
        self.database = sqlite3.connect(':memory:')
        self.statements = []

    def cursor(self):
        return LocalCursor(self)


@pytest.fixture
def source():
    source = LocalSource()
    source.database.executescript('''
        CREATE TABLE orders (
            order_key INTEGER, customer_key INTEGER, customer_name TEXT,
            order_status TEXT, total_price TEXT, order_date TEXT,
            source_version INTEGER, source_updated_at TEXT, event_type TEXT);
        CREATE TABLE orders_changes AS SELECT * FROM orders;
        CREATE TABLE sample_orders (
            o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus TEXT,
            o_totalprice TEXT, o_orderdate TEXT);
        CREATE TABLE sample_customers (c_custkey INTEGER, c_name TEXT);
        INSERT INTO sample_customers VALUES (10, 'Sample customer');
        INSERT INTO orders VALUES
            (1, 10, 'Modified customer', 'O', '999.00', '2020-01-01', 2, '2030-01-01T00:00:00.000000+00:00', 'update'),
            (4, 10, 'Simulator customer', 'F', '777.00', '2020-01-01', 1, '2029-01-01T00:00:00.000000+00:00', 'insert'),
            (99, 10, 'Simulator customer', 'F', '888.00', '2020-01-01', 1, '2029-01-01T00:00:00.000000+00:00', 'insert');
        INSERT INTO orders_changes SELECT * FROM orders;
        INSERT INTO orders_changes VALUES
            (1, 10, 'Sample customer', 'O', '10.00', '2020-01-01', 1, '2029-01-01T00:00:00.000000+00:00', 'insert');
    ''')
    source.database.executemany('INSERT INTO sample_orders VALUES (?,10,?,?,?)', [
        (key, 'P', f'{key}.25', '2021-02-03') for key in range(1, 7)
    ])
    source.database.commit()
    yield source
    source.database.close()


def test_append_skips_existing_keys_preserves_orders_and_advances_journal(source):
    existing = source.database.execute('SELECT * FROM orders ORDER BY order_key').fetchall()
    assert demo.append_orders(source, SETTINGS, 2) == (2, 5)
    assert source.database.execute('SELECT * FROM orders WHERE order_key IN (1,4,99) ORDER BY order_key').fetchall() == existing
    added = source.database.execute('SELECT * FROM orders WHERE order_key IN (2,3) ORDER BY order_key').fetchall()
    assert [(row[0], row[2], row[3], row[4], row[5], row[6], row[8]) for row in added] == [
        (key, 'Sample customer', 'P', f'{key}.25', '2021-02-03', 1, 'insert') for key in (2, 3)
    ]
    assert all(datetime.fromisoformat(row[7]) > datetime(2030, 1, 1, tzinfo=timezone.utc) for row in added)
    assert source.database.execute('SELECT * FROM orders_changes WHERE order_key IN (2,3) ORDER BY order_key').fetchall() == added
    assert not source.database.in_transaction


def test_repeated_append_takes_remaining_orders_and_exhaustion_is_a_noop(source):
    assert demo.append_orders(source, SETTINGS, 2) == (2, 5)
    assert demo.append_orders(source, SETTINGS, 10) == (2, 7)
    before = source.database.execute('SELECT * FROM orders_changes').fetchall()
    assert demo.append_orders(source, SETTINGS, 10) == (0, 7)
    assert source.database.execute('SELECT * FROM orders_changes').fetchall() == before
    assert source.database.execute('SELECT order_key FROM orders ORDER BY order_key').fetchall() == [(key,) for key in (1, 2, 3, 4, 5, 6, 99)]
    assert source.database.execute('SELECT COUNT(*) FROM orders_changes').fetchone()[0] == 8


def test_journal_failure_leaves_current_insert_uncommitted(source):
    source.database.executescript('''CREATE TRIGGER reject_journal BEFORE INSERT ON orders_changes
        BEGIN SELECT RAISE(ABORT, 'journal unavailable'); END;''')
    with pytest.raises(sqlite3.IntegrityError, match='journal unavailable'):
        demo.append_orders(source, SETTINGS, 2)
    assert 'COMMIT' not in source.statements
    source.database.rollback()
    assert source.database.execute('SELECT COUNT(*) FROM orders').fetchone()[0] == 3
    assert source.database.execute('SELECT COUNT(*) FROM orders_changes').fetchone()[0] == 4


@pytest.mark.parametrize('empty_table', ['orders', 'orders_changes'])
def test_append_refuses_unprepared_source_before_transaction(source, empty_table):
    source.database.execute(f'DELETE FROM {empty_table}')
    source.database.commit()
    with pytest.raises(demo.AppendRefused, match='existing prepared source'):
        demo.append_orders(source, SETTINGS, 2)
    assert 'BEGIN' not in source.statements
    assert not any(sql.startswith('INSERT') for sql in source.statements)
