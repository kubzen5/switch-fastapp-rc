"""Explicit local maintenance command; never called during service startup."""
import argparse
import json

from psycopg import sql

from app.config import get_settings
from app.db.connection import connect

SINK_TABLES = ('orders_current', 'event_rejections', 'processed_events', 'event_deliveries')
PIPELINE_TABLES = SINK_TABLES + (
    'source_quarantine', 'source_entity_versions', 'sync_state', 'sync_batches', 'sync_runs',
)
AUDIT_TRIGGERS = {
    'event_deliveries': 'delivery_append_only',
    'event_rejections': 'rejection_append_only',
    'processed_events': 'processed_append_only',
}


def clean(connection, *, execute=False, scope='sink'):
    """Inspect first, then optionally clear known tables atomically.

    ACCESS EXCLUSIVE locks prevent writes between counting and truncating.
    The caller must stop adapter/consumer before maintenance to avoid their
    writes resuming with stale in-memory state after the locks are released.
    """
    if scope not in ('sink', 'pipeline'):
        raise ValueError('Unknown cleanup scope')
    tables = SINK_TABLES if scope == 'sink' else PIPELINE_TABLES
    with connection.transaction():
        connection.execute('SET LOCAL lock_timeout = \'5s\'')
        # Share the migration lock, preventing a schema change while inspecting.
        connection.execute('SELECT pg_advisory_xact_lock(72401001)')
        schema = connection.execute('SELECT current_schema() AS name').fetchone()['name']
        existing = []
        missing = []
        for table in tables:
            row = connection.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema=%s "
                "AND table_name=%s AND table_type='BASE TABLE'", (schema, table),
            ).fetchone()
            (existing if row else missing).append(table)
        qualified = [sql.Identifier(schema, table) for table in existing]
        if existing:
            connection.execute(sql.SQL('LOCK TABLE {} IN {} MODE').format(
                sql.SQL(', ').join(qualified),
                sql.SQL('ACCESS EXCLUSIVE' if execute else 'ACCESS SHARE'),
            ))
        counts = {}
        for table, identifier in zip(existing, qualified):
            counts[table] = connection.execute(
                sql.SQL('SELECT count(*) AS count FROM {}').format(identifier)
            ).fetchone()['count']
        report = {'scope': scope, 'before': counts, 'missing_tables': missing,
                  'cleared': False, 'after': dict(counts)}
        if not execute or not any(counts.values()):
            return report
        # Do not clear a partial installation; no CASCADE into unknown tables.
        if missing:
            raise RuntimeError('Cleanup refused: migrate the incomplete schema first')
        if scope == 'pipeline':
            fenced = connection.execute(
                sql.SQL('SELECT 1 FROM {} WHERE blocked LIMIT 1').format(
                    sql.Identifier(schema, 'source_write_fences')
                )
            ).fetchone()
            if fenced:
                raise RuntimeError('Cleanup refused: resolve the source write fence first')
        # Audit mutation remains forbidden outside this single locked transaction.
        # PostgreSQL rolls back trigger changes and TRUNCATE together on failure.
        for table, trigger in AUDIT_TRIGGERS.items():
            connection.execute(sql.SQL('ALTER TABLE {} DISABLE TRIGGER {}').format(
                sql.Identifier(schema, table), sql.Identifier(trigger),
            ))
        connection.execute(sql.SQL('TRUNCATE TABLE {} CONTINUE IDENTITY').format(
            sql.SQL(', ').join(qualified)
        ))
        for table, trigger in AUDIT_TRIGGERS.items():
            connection.execute(sql.SQL('ALTER TABLE {} ENABLE TRIGGER {}').format(
                sql.Identifier(schema, table), sql.Identifier(trigger),
            ))
        report['cleared'] = True
        report['after'] = {table: 0 for table in existing}
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true', help='Actually delete stored data')
    parser.add_argument('--scope', choices=('sink', 'pipeline'), default='sink')
    args = parser.parse_args()
    with connect(get_settings()) as connection:
        report = clean(connection, execute=args.execute, scope=args.scope)
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
