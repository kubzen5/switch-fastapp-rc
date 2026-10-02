import hashlib
from pathlib import Path

from app.config import get_settings
from app.db.connection import connect
from app.runtime import wait_for_postgres


def migrate() -> None:
    settings = get_settings()
    wait_for_postgres(settings)
    with connect(settings) as connection:
        connection.execute("SELECT pg_advisory_xact_lock(72401001)")
        connection.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version text PRIMARY KEY,
                checksum text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
        """)
        for path in sorted(Path(__file__).with_name("migrations").glob("*.sql")):
            sql = path.read_text()
            checksum = hashlib.sha256(sql.encode()).hexdigest()
            existing = connection.execute(
                "SELECT checksum FROM schema_migrations WHERE version = %s", (path.name,)
            ).fetchone()
            if existing:
                if existing["checksum"] != checksum:
                    raise RuntimeError("Applied migration checksum mismatch")
                continue
            connection.execute(sql)
            connection.execute(
                "INSERT INTO schema_migrations(version, checksum) VALUES (%s, %s)",
                (path.name, checksum),
            )


if __name__ == "__main__":
    from app.logging import configure_logging
    from app.runtime import guarded_main

    configure_logging(get_settings().log_level)
    guarded_main(migrate)
