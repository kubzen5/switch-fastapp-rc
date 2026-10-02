import json
import logging
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        result = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        # Only explicit correlation fields; exception messages can contain credentials.
        for field in ("event_id", "batch_id", "entity_key", "source", "topic", "partition", "offset", "error_type", "attempt", "retry_delay_seconds", "pending_count", "records_seen"):
            if hasattr(record, field):
                result[field] = getattr(record, field)
        return json.dumps(result, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)
    for name in ("snowflake.connector", "confluent_kafka"):
        logging.getLogger(name).setLevel(logging.WARNING)
