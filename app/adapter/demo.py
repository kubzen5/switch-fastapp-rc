"""Controlled source writer. Ambiguous failures retain the durable PG fence."""
import argparse
from contextlib import closing
from datetime import datetime, timezone, timedelta

from app.config import get_settings
from app.adapter.coordination import source_lock
from app.adapter.snowflake_client import connect_source, source_table, SnowflakeReader


class PrepareRefused(RuntimeError):
    """Existing rows detected before BEGIN or any source data write."""


def execute(conn, sql, params=()):
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description else []


def prepare(conn, settings, count):
    table = source_table(settings)
    database, schema, _ = table.split('.')
    # DDL must precede the explicit data transaction (Snowflake DDL commits).
    execute(conn, f'CREATE DATABASE IF NOT EXISTS {database}')
    execute(conn, f'CREATE SCHEMA IF NOT EXISTS {database}.{schema}')
    execute(conn, f'''CREATE TABLE IF NOT EXISTS {table} (
        order_key NUMBER(18,0) NOT NULL, customer_key NUMBER(18,0), customer_name VARCHAR,
        order_status VARCHAR, total_price NUMBER(18,2), order_date DATE,
        source_version NUMBER(18,0) NOT NULL, source_updated_at TIMESTAMP_TZ(6) NOT NULL,
        event_type VARCHAR NOT NULL)''')
    execute(conn, f'CREATE TABLE IF NOT EXISTS {table}_CHANGES LIKE {table}')
    current_rows = execute(conn, f'SELECT COUNT(*) FROM {table}')[0][0]
    journal_rows = execute(conn, f'SELECT COUNT(*) FROM {table}_CHANGES')[0][0]
    if current_rows or journal_rows:
        raise PrepareRefused(
            f'Prepare requires empty tables: {table} has {current_rows} rows; '
            f'{table}_CHANGES has {journal_rows} rows. No source data was changed. '
            'For an existing demo, skip prepare and check http://localhost:8000/stats. '
            'For a fresh demo, use docs/demo-recording.md; never reset a synced source.'
        )
    execute(conn, 'BEGIN')
    execute(conn, f'''INSERT INTO {table}
        SELECT o.o_orderkey,c.c_custkey,c.c_name,o.o_orderstatus,o.o_totalprice,o.o_orderdate,
        1,%s,'insert' FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS o
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER c ON o.o_custkey=c.c_custkey
        ORDER BY o.o_orderkey LIMIT %s''', (datetime.now(timezone.utc), count))
    execute(conn, f'INSERT INTO {table}_CHANGES SELECT * FROM {table}')
    execute(conn, 'COMMIT')


def epoch(conn, settings):
    high = SnowflakeReader(conn, settings).high_watermark()
    return max(datetime.now(timezone.utc), high.source_updated_at + timedelta(microseconds=1)) if high else datetime.now(timezone.utc)


def mutate(conn, settings):
    table = source_table(settings)
    keys = execute(conn, f'SELECT order_key FROM {table} ORDER BY order_key LIMIT 3')
    if len(keys) < 3:
        raise RuntimeError('Run prepare first')
    execute(conn, 'BEGIN')
    stamp = epoch(conn, settings)
    new_key = execute(conn, f'SELECT MAX(order_key)+1 FROM {table}')[0][0]
    execute(conn, f'''INSERT INTO {table} SELECT %s,customer_key,customer_name,order_status,total_price,order_date,1,%s,'insert'
        FROM {table} WHERE order_key=%s''', (new_key, stamp, keys[0][0]))
    execute(conn, f'''UPDATE {table} SET total_price=total_price+1,source_version=source_version+1,
        source_updated_at=%s,event_type='update' WHERE order_key IN (%s,%s,%s)''', (stamp, *(key[0] for key in keys)))
    execute(conn, f'INSERT INTO {table}_CHANGES SELECT * FROM {table} WHERE source_updated_at=%s', (stamp,))
    # Same entity updated again before the adapter reads: preserve BOTH versions.
    second_stamp = stamp + timedelta(microseconds=1)
    execute(conn, f'''UPDATE {table} SET total_price=total_price+2,source_version=source_version+1,
        source_updated_at=%s,event_type='update' WHERE order_key=%s''', (second_stamp, keys[0][0]))
    execute(conn, f'INSERT INTO {table}_CHANGES SELECT * FROM {table} WHERE source_updated_at=%s', (second_stamp,))
    execute(conn, 'COMMIT')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['prepare', 'mutate'])
    parser.add_argument('--rows', type=int, default=20000)
    args = parser.parse_args()
    if not 10000 <= args.rows <= 50000:
        parser.error('--rows must be between 10000 and 50000')
    settings = get_settings()
    with source_lock(settings) as coordinator:
        # Persist BEFORE any Snowflake action. Ambiguous failures stay fenced.
        coordinator.execute('UPDATE source_write_fences SET blocked=true WHERE source=%s', (settings.source_name,))
        try:
            with closing(connect_source(settings, use_database=args.action != 'prepare')) as conn:
                if args.action == 'prepare':
                    prepare(conn, settings, args.rows)
                else:
                    mutate(conn, settings)
        except PrepareRefused as error:
            # The preflight refused before data writes, and close succeeded.
            # source_lock already refused any fence from an earlier writer.
            coordinator.execute('UPDATE source_write_fences SET blocked=false WHERE source=%s', (settings.source_name,))
            parser.exit(1, f'{error}\nSource fence cleared after prepare refusal.\n')
        coordinator.execute('UPDATE source_write_fences SET blocked=false WHERE source=%s', (settings.source_name,))
    print(f'{args.action} committed; source fence cleared')


if __name__ == '__main__':
    main()
