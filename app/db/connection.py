import psycopg
from psycopg.rows import dict_row

from app.config import Settings


def connect(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        connect_timeout=5,
        options="-c timezone=UTC -c statement_timeout=10000",
        row_factory=dict_row,
    )
