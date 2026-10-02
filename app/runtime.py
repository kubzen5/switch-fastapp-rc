import logging
from pathlib import Path
import signal
import threading
import time
from collections.abc import Callable

from confluent_kafka.admin import AdminClient

from app.config import Settings
from app.db.connection import connect

logger = logging.getLogger(__name__)


def wait_until(check: Callable[[], None], timeout: int) -> None:
    deadline = time.monotonic() + timeout
    delay = 0.5
    while True:
        try:
            check()
            return
        except Exception:
            if time.monotonic() >= deadline:
                raise RuntimeError("Dependency readiness timeout") from None
            time.sleep(min(delay, max(0, deadline - time.monotonic())))
            delay = min(delay * 2, 5)


def check_postgres(settings: Settings, require_schema: bool = True) -> None:
    with connect(settings) as connection:
        connection.execute("SELECT 1")
        if require_schema:
            row = connection.execute("SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1").fetchone()
            expected = max(path.name for path in (Path(__file__).parent / "db" / "migrations").glob("*.sql"))
            if not row or row["version"] != expected:
                raise RuntimeError("Database migration missing")


def check_broker(settings: Settings) -> None:
    metadata = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers}).list_topics(timeout=5)
    topic = metadata.topics.get(settings.kafka_topic)
    if not metadata.brokers or topic is None or topic.error is not None:
        raise RuntimeError("Broker or topic unavailable")


def wait_for_postgres(settings: Settings, require_schema: bool = False) -> None:
    wait_until(lambda: check_postgres(settings, require_schema), settings.dependency_timeout_seconds)


def wait_for_dependencies(settings: Settings) -> None:
    wait_for_postgres(settings, require_schema=True)
    wait_until(lambda: check_broker(settings), settings.dependency_timeout_seconds)


def stop_event() -> threading.Event:
    stopped = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stopped.set())
    return stopped


def guarded_main(run: Callable[[], None]) -> None:
    try:
        run()
    except Exception as error:
        logger.error("service_failed", extra={"error_type": type(error).__name__})
        raise SystemExit(1) from None
