from app.config import get_settings
from app.consumer.materializer import Delivery, materialize
from app.db.connection import connect
from app.healthcheck import heartbeat
from app.logging import configure_logging
from app.runtime import guarded_main, stop_event, wait_for_dependencies
from app.transport.consumer import create_consumer
from app.consumer.coordination import consumer_lock


def process_message(consumer, connection, message) -> str:
    disposition = materialize(connection, Delivery(
        message.topic(), message.partition(), message.offset(), message.key(), message.value()
    ))
    # Failure here leaves a committed delivery. Redelivery is audited as a
    # duplicate, with no second application of the logical change.
    consumer.commit(message=message, asynchronous=False)
    return disposition


def main() -> None:
    settings = get_settings()
    stopped = stop_event()
    wait_for_dependencies(settings)
    with consumer_lock(settings):
        consume(settings, stopped)


def consume(settings, stopped) -> None:
    consumer = create_consumer(settings)
    try:
        while not stopped.is_set():
            heartbeat()
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("Broker consumption failed")
            # A failure exits without offset commit; restart resumes/redelivers.
            with connect(settings) as connection:
                process_message(consumer, connection, message)
    finally:
        consumer.close()


if __name__ == "__main__":
    configure_logging(get_settings().log_level)
    guarded_main(main)
