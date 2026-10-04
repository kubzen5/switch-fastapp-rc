"""Generate new source orders through the controlled writer protocol."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import math
import time

from app.adapter.coordination import source_lock
from app.adapter.demo import epoch, execute
from app.adapter.snowflake_client import connect_source, source_table
from app.config import get_settings
from app.logging import configure_logging
from app.runtime import guarded_main, stop_event


BATCH_SIZE = 5


def insert_orders(connection, settings):
    """Atomically append five new version-1 orders and their journal entries."""
    table = source_table(settings)
    seed, maximum = execute(
        connection, f'SELECT MIN(order_key), MAX(order_key) FROM {table}'
    )[0]
    if seed is None:
        raise RuntimeError('Simulator requires an existing prepared source')
    first_key = int(maximum) + 1
    last_key = first_key + BATCH_SIZE - 1
    stamp = epoch(connection, settings)
    execute(connection, 'BEGIN')
    # Reuse a valid existing customer; every order has a new key and version 1.
    execute(connection, f'''INSERT INTO {table}
        SELECT %s + numbers.column1, customer_key, customer_name, order_status,
            total_price, CURRENT_DATE(), 1, %s, 'insert'
        FROM {table} CROSS JOIN (VALUES (0), (1), (2), (3), (4)) AS numbers
        WHERE order_key = %s''', (first_key, stamp, seed))
    execute(connection, f'''INSERT INTO {table}_CHANGES
        SELECT * FROM {table} WHERE order_key BETWEEN %s AND %s''',
        (first_key, last_key))
    execute(connection, 'COMMIT')
    return first_key, last_key


def write_batch(settings, stopped):
    # Release the shared source lock between batches so the adapter can sync.
    with source_lock(settings) as coordinator:
        if stopped.is_set():
            return None
        coordinator.execute(
            'UPDATE source_write_fences SET blocked=true WHERE source=%s',
            (settings.source_name,),
        )
        with closing(connect_source(settings)) as connection:
            keys = insert_orders(connection, settings)
        # As with the existing writer, uncertain writes/close failures retain
        # the fence. Signals request a stop without interrupting this commit.
        coordinator.execute(
            'UPDATE source_write_fences SET blocked=false WHERE source=%s',
            (settings.source_name,),
        )
    return keys


def simulate(settings, stopped, interval, batches):
    completed = 0
    while not stopped.is_set() and (batches == 0 or completed < batches):
        started = time.monotonic()
        keys = write_batch(settings, stopped)
        if keys is None:
            break
        completed += 1
        elapsed = time.monotonic() - started
        stamp = datetime.now(timezone.utc).isoformat(timespec='seconds')
        print(
            f'{stamp} batch={completed} committed={BATCH_SIZE} '
            f'orders=order:{keys[0]}..order:{keys[1]} '
            f'total_new_orders={completed * BATCH_SIZE} batch_seconds={elapsed:.2f}',
            flush=True,
        )
        if elapsed > interval:
            print(f'Batch exceeded the {interval:g}s target interval; no catch-up burst.', flush=True)
        if batches and completed >= batches:
            break
        stopped.wait(max(0, interval - elapsed))
    print(f'Simulator stopped: {completed} batches, {completed * BATCH_SIZE} new orders.', flush=True)


def main():
    parser = argparse.ArgumentParser(description='Insert five new Snowflake orders per interval.')
    parser.add_argument('--interval', type=float, default=4, help='Target seconds between batch starts (default: 4).')
    parser.add_argument('--batches', type=int, default=0, help='Stop after this many batches; 0 runs until Ctrl+C.')
    args = parser.parse_args()
    if not math.isfinite(args.interval) or args.interval <= 0:
        parser.error('--interval must be finite and positive')
    if args.batches < 0:
        parser.error('--batches must be nonnegative')
    simulate(get_settings(), stop_event(), args.interval, args.batches)


if __name__ == '__main__':
    configure_logging(get_settings().log_level)
    guarded_main(main)
