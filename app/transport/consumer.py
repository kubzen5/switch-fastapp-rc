from confluent_kafka import Consumer

from app.config import Settings


def create_consumer(settings: Settings) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "max.poll.interval.ms": 300000,
    })
    consumer.subscribe([settings.kafka_topic])
    return consumer
