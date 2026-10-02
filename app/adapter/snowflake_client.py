import re

import snowflake.connector

from app.config import Settings


def source_table(settings: Settings) -> str:
    parts = (settings.snowflake_database, settings.snowflake_schema, settings.snowflake_table)
    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in parts):
        raise ValueError("Only plain Snowflake identifiers are supported")
    return ".".join(part.upper() for part in parts)


def connect_source(settings: Settings, *, use_database: bool = True):
    if not all((settings.snowflake_account, settings.snowflake_user, settings.snowflake_password.get_secret_value(), settings.snowflake_warehouse)):
        raise RuntimeError("Snowflake configuration is incomplete")
    return snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password.get_secret_value(),
        warehouse=settings.snowflake_warehouse,
        **({"database": settings.snowflake_database, "schema": settings.snowflake_schema} if use_database else {}),
        **({"role": settings.snowflake_role} if settings.snowflake_role else {}),
        session_parameters={"TIMEZONE": "UTC"},
        login_timeout=15,
        network_timeout=30,
    )


class SnowflakeReader:
    def __init__(self, connection, settings):
        self.connection = connection
        self.table = source_table(settings) + '_CHANGES'

    def query(self, sql, params=()):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            names = [column[0].lower() for column in cursor.description]
            return [dict(zip(names, row)) for row in cursor.fetchall()]

    def high_watermark(self):
        from app.domain.cursor import SyncCursor
        rows = self.query(f'SELECT source_updated_at, order_key FROM {self.table} ORDER BY source_updated_at DESC, order_key DESC LIMIT 1')
        return SyncCursor(**rows[0]) if rows else None

    def read_batch(self, after, high_watermark, limit):
        predicate = ''
        params = []
        if after:
            predicate = 'AND (source_updated_at > %s OR (source_updated_at = %s AND order_key > %s))'
            params.extend([after.source_updated_at, after.source_updated_at, after.order_key])
        params.extend([high_watermark.source_updated_at, high_watermark.source_updated_at, high_watermark.order_key, limit])
        # Window evaluation precedes LIMIT: a duplicate cursor straddling the
        # page boundary must fail before either record is checkpointed.
        rows = self.query(f'''SELECT *, COUNT(*) OVER
            (PARTITION BY source_updated_at, order_key) AS cursor_multiplicity
            FROM {self.table} WHERE 1=1 {predicate}
            AND (source_updated_at < %s OR (source_updated_at = %s AND order_key <= %s))
            ORDER BY source_updated_at, order_key LIMIT %s''', tuple(params))
        for row in rows:
            if row.pop('cursor_multiplicity') != 1:
                raise RuntimeError('Source journal contains duplicate cursors; checkpoint retained')
        return rows
