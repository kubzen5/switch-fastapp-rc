import logging
from contextlib import closing

from app.config import get_settings
from app.healthcheck import heartbeat
from app.logging import configure_logging
from app.runtime import guarded_main, stop_event, wait_for_dependencies
from app.db.connection import connect
from app.adapter.coordination import source_lock
from app.adapter.snowflake_client import connect_source, SnowflakeReader
from app.adapter.checkpoint import PostgresCheckpoint
from app.adapter.sync import run_sync
from app.transport.producer import EventProducer, PublicationStopped


def sync_once(settings, stopped=None):
    with source_lock(settings), closing(connect_source(settings)) as snowflake, connect(settings) as postgres:
        postgres.autocommit = True

        def pulse():
            heartbeat()
            if stopped is not None and stopped.is_set():
                raise PublicationStopped('Shutdown requested; checkpoint retained')

        with EventProducer(settings, stopped) as publisher:
            return run_sync(SnowflakeReader(snowflake, settings), PostgresCheckpoint(postgres), publisher, settings.source_name, settings.sync_batch_size, pulse)


def main():
    settings = get_settings()
    stopped = stop_event()
    wait_for_dependencies(settings)
    while not stopped.is_set():
        heartbeat()
        try:
            count = sync_once(settings, stopped)
            logging.getLogger(__name__).info('sync_completed', extra={'source': settings.source_name, 'records_seen': count})
        except Exception as error:
            # Never log connector errors containing credentials or raw payloads.
            logging.getLogger(__name__).error('sync_failed', extra={'source': settings.source_name, 'error_type': type(error).__name__})
        stopped.wait(settings.sync_interval_seconds)


if __name__ == '__main__':
    configure_logging(get_settings().log_level)
    guarded_main(main)
