import sys
import time
from pathlib import Path

from app.config import get_settings
from app.runtime import check_broker, check_postgres

HEARTBEAT = Path("/tmp/worker-heartbeat")


def heartbeat() -> None:
    HEARTBEAT.touch()


def main() -> None:
    try:
        if sys.argv[1] == "worker" and (
            not HEARTBEAT.exists() or time.time() - HEARTBEAT.stat().st_mtime > 30
        ):
            raise RuntimeError("Worker heartbeat expired")
        settings = get_settings()
        check_postgres(settings)
        check_broker(settings)
    except Exception:
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
