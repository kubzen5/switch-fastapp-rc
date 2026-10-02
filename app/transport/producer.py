"""Sequential ACK publication; uncertain delivery requires checkpoint replay."""
import logging
import random
import threading
import time
from confluent_kafka import KafkaException, Producer
from app.config import Settings
from app.domain.envelope import EventEnvelope

logger = logging.getLogger(__name__)


class PublicationFailed(RuntimeError):
    """Delivery failed or is uncertain; retain the checkpoint."""


class PublicationStopped(PublicationFailed):
    pass


class EventProducer:
    def __init__(self, settings: Settings, stopped=None, *, producer=None):
        self.settings = settings
        self.topic = settings.kafka_topic
        self.stopped = stopped if stopped is not None else threading.Event()
        self.closed = False
        self.drained = False
        self.context = {}
        self.producer = producer if producer is not None else Producer({
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'enable.idempotence': True, 'acks': 'all',
            'delivery.timeout.ms': settings.kafka_delivery_timeout_ms,
            'request.timeout.ms': min(5000, settings.kafka_delivery_timeout_ms),
            'retry.backoff.ms': 500, 'retry.backoff.max.ms': 5000,
        })

    def publish(self, event: EventEnvelope) -> None:
        if self.closed:
            raise PublicationFailed('Producer closed')
        value, key = event.model_dump_json().encode(), event.partition_key.encode()
        context = dict(event_id=event.event_id, batch_id=str(event.batch_id),
                       entity_key=event.entity_key, source=event.source, topic=self.topic)
        self.context = context
        for attempt in range(1, self.settings.kafka_publish_attempts + 1):
            if self.stopped.is_set():
                raise PublicationStopped('Shutdown requested; checkpoint retained')
            result = []

            def delivered(error, message):
                result.append((error, message))

            try:
                self.producer.produce(self.topic, key=key, value=value, on_delivery=delivered)
            except (BufferError, KafkaException):
                self.producer.poll(0)
                result.append((True, None))
            deadline = time.monotonic() + self.settings.kafka_delivery_timeout_ms / 1000 + 1
            while not result and time.monotonic() < deadline:
                # Finish the current delivery on SIGTERM, within its timeout.
                self.producer.poll(min(0.1, max(0, deadline - time.monotonic())))
            if not result:
                # Do not enqueue a retry while the previous outcome is unresolved.
                self.closed = True
                logger.error('publication_unconfirmed', extra={**context, 'attempt': attempt})
                raise PublicationFailed('Delivery callback timeout; checkpoint retained')
            error, message = result[0]
            if error is None:
                logger.info('event_published', extra={**context, 'attempt': attempt,
                            'partition': message.partition(), 'offset': message.offset()})
                return
            if self.stopped.is_set():
                raise PublicationStopped('Shutdown during delivery; checkpoint retained')
            if hasattr(error, 'fatal') and error.fatal():
                break
            if attempt == self.settings.kafka_publish_attempts:
                break
            delay = random.uniform(0, min(self.settings.kafka_retry_max_seconds,
                                         self.settings.kafka_retry_base_seconds * 2 ** (attempt - 1)))
            logger.warning('publication_retry', extra={**context, 'attempt': attempt, 'retry_delay_seconds': delay})
            if self.stopped.wait(delay):
                raise PublicationStopped('Shutdown during retry; checkpoint retained')
        logger.error('publication_exhausted', extra={**context, 'attempt': attempt})
        raise PublicationFailed('Publication retries exhausted; checkpoint retained')

    def close(self):
        if self.drained:
            return
        self.drained = True
        self.closed = True
        remaining = self.producer.flush(self.settings.kafka_shutdown_timeout_seconds)
        if remaining:
            self.producer.purge(in_queue=True, in_flight=True, blocking=False)
            self.producer.poll(0)
            logger.error('producer_pending_discarded', extra={**self.context, 'pending_count': remaining, 'topic': self.topic})

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
