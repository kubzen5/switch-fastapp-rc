"""Destructive fault injection ONLY for an isolated disposable test stack.

Run phases: setup, before, resume-before, partial, resume-partial, lost-ack,
crash, resume-crash, report. Settings use normal environment variables.
partial uses REVIEW_SIGNAL_DIR to coordinate stopping the external broker.
Source is a deterministic in-memory journal; Kafka and checkpoints are real.
"""
import json
import os
from pathlib import Path
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from app.adapter.checkpoint import PostgresCheckpoint
from app.adapter.sync import run_sync
from app.config import get_settings
from app.consumer.materializer import Delivery, materialize
from app.db.connection import connect
from app.db.migrate import migrate
from app.domain.cursor import SyncCursor
from app.logging import configure_logging
from app.transport.producer import EventProducer, PublicationFailed

STAMP = datetime(2026, 10, 2, tzinfo=timezone.utc)


def record(key):
    return dict(order_key=key, customer_key=1, customer_name='Review',
                order_status='O', total_price=Decimal('10.00'), order_date=STAMP.date(),
                source_version=1, source_updated_at=STAMP, event_type='insert')


def cursor(key):
    return SyncCursor(source_updated_at=STAMP, order_key=key)


class Journal:
    def __init__(self, keys):
        self.keys = keys

    def high_watermark(self):
        return cursor(self.keys[-1])

    def read_batch(self, after, high, limit):
        return [record(k) for k in self.keys if (after is None or cursor(k).follows(after))
                and not cursor(k).follows(high)][:limit]


def sync(store, publisher, source, keys):
    return run_sync(Journal(keys), store, publisher, source, 2)


def checkpoint(store, source, key):
    assert store.load(source) == (cursor(key) if key else None)


class SuppressAck:
    """Fault at callback boundary: actual broker ACK becomes an application timeout.

    This proves broker writes + uncertain application outcome; it is NOT a
    network proxy or evidence of a TCP response getting lost in transit.
    """
    def __init__(self, client):
        self.client = client
        self.confirmed_writes = 0

    def produce(self, *args, on_delivery, **kwargs):
        def delivered(error, message):
            if error is None:
                self.confirmed_writes += 1
                error = KafkaError(KafkaError._MSG_TIMED_OUT, 'injected lost ACK')
            on_delivery(error, message)
        return self.client.produce(*args, on_delivery=delivered, **kwargs)

    def __getattr__(self, name):
        return getattr(self.client, name)


def report(settings, connection):
    consumer = Consumer({'bootstrap.servers': settings.kafka_bootstrap_servers,
                         'group.id': 'isolated-review-report', 'enable.auto.commit': False})
    try:
        metadata = consumer.list_topics(settings.kafka_topic, timeout=10)
        partitions = []
        ends = {}
        for number in metadata.topics[settings.kafka_topic].partitions:
            partition = TopicPartition(settings.kafka_topic, number)
            low, high = consumer.get_watermark_offsets(partition, timeout=10)
            partitions.append(TopicPartition(settings.kafka_topic, number, low))
            ends[number] = high
        consumer.assign(partitions)
        positions = {p.partition: p.offset for p in partitions}
        deliveries = []
        deadline = time.monotonic() + 30
        while any(positions[p] < ends[p] for p in ends):
            assert time.monotonic() < deadline, 'Consumption timeout'
            message = consumer.poll(1)
            if message is None:
                continue
            assert not message.error(), str(message.error())
            positions[message.partition()] = message.offset() + 1
            envelope = json.loads(message.value())
            disposition = materialize(connection, Delivery(message.topic(), message.partition(),
                                      message.offset(), message.key(), message.value()))
            deliveries.append((envelope, message.partition(), disposition))
        counts = {}
        for scenario, expected in [('before', 3), ('partial', 4), ('lost', 3), ('crash', 4)]:
            items = [d for d in deliveries if d[0]['source'] == 'review.' + scenario]
            assert len(items) == expected, (scenario, len(items))
            counts[scenario] = {'deliveries': len(items),
                                'unique_event_ids': len({d[0]['event_id'] for d in items}),
                                'duplicates': sum(d[2] == 'duplicate' for d in items)}
            grouped = {}
            for envelope, partition, _ in items:
                grouped.setdefault(envelope['entity_key'], set()).add(partition)
            assert all(len(partitions) == 1 for partitions in grouped.values())
        assert counts['partial']['duplicates'] == 1
        assert counts['lost']['duplicates'] == 2
        assert counts['crash']['duplicates'] == 2
        state = connection.execute("SELECT count(*) AS n FROM orders_current WHERE source LIKE 'review.%'").fetchone()['n']
        assert state == 9, state
        print(json.dumps({'result': 'PASS', 'scenarios': counts, 'current_entities': state}))
    finally:
        consumer.close()


def main():
    phase = sys.argv[1]
    settings = get_settings()
    configure_logging('INFO')
    if phase == 'setup':
        migrate()
        admin = AdminClient({'bootstrap.servers': settings.kafka_bootstrap_servers})
        for result in admin.create_topics([NewTopic(settings.kafka_topic, num_partitions=3, replication_factor=1)]).values():
            result.result(timeout=20)
        with connect(settings) as connection:
            connection.autocommit = True
            store = PostgresCheckpoint(connection)
            with EventProducer(settings) as publisher:
                for source in ('review.before', 'review.partial'):
                    sync(store, publisher, source, [1])
                    checkpoint(store, source, 1)
        print('SETUP_PASS', flush=True)
        return
    with connect(settings) as connection:
        connection.autocommit = True
        store = PostgresCheckpoint(connection)
        if phase == 'report':
            report(settings, connection)
            return
        if phase == 'before':
            with EventProducer(settings) as publisher:
                try:
                    sync(store, publisher, 'review.before', [1, 2, 3])
                except PublicationFailed:
                    checkpoint(store, 'review.before', 1)
                else:
                    raise AssertionError('Outage unexpectedly delivered')
        elif phase == 'partial':
            directory = Path(os.environ['REVIEW_SIGNAL_DIR'])
            with EventProducer(settings) as publisher:
                class PauseAfterAck:
                    def publish(self, event):
                        publisher.publish(event)
                        if event.entity_key == 'order:2':
                            (directory / 'first-ack').touch()
                            deadline = time.monotonic() + 60
                            while not (directory / 'broker-stopped').exists():
                                assert time.monotonic() < deadline, 'Coordinator timeout'
                                time.sleep(0.1)
                try:
                    sync(store, PauseAfterAck(), 'review.partial', [1, 2, 3])
                except PublicationFailed:
                    checkpoint(store, 'review.partial', 1)
                else:
                    raise AssertionError('Mid-batch outage unexpectedly delivered')
        elif phase == 'lost-ack':
            with EventProducer(settings) as publisher:
                suppressed = SuppressAck(publisher.producer)
                publisher.producer = suppressed
                try:
                    sync(store, publisher, 'review.lost', [1])
                except PublicationFailed:
                    checkpoint(store, 'review.lost', None)
                    assert suppressed.confirmed_writes == settings.kafka_publish_attempts
                else:
                    raise AssertionError('Suppressed ACK unexpectedly succeeded')
            with EventProducer(settings) as publisher:
                sync(store, publisher, 'review.lost', [1])
                checkpoint(store, 'review.lost', 1)
        elif phase == 'crash':
            class CrashStore(PostgresCheckpoint):
                def commit_batch(self, *args):
                    os._exit(77)  # Actual process exit after all publication ACKs.
            with EventProducer(settings) as publisher:
                sync(CrashStore(connection), publisher, 'review.crash', [1, 2])
        elif phase.startswith('resume-'):
            scenario = phase.removeprefix('resume-')
            source = 'review.' + scenario
            keys = [1, 2] if scenario == 'crash' else [1, 2, 3]
            checkpoint(store, source, None if scenario == 'crash' else 1)
            with EventProducer(settings) as publisher:
                sync(store, publisher, source, keys)
                checkpoint(store, source, keys[-1])
                assert sync(store, publisher, source, keys) == 0
        else:
            raise AssertionError('Unknown phase')
    print(phase.upper() + '_PASS', flush=True)


if __name__ == '__main__':
    main()
