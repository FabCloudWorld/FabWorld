import json
import os

from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "sensor-raw"

_producer: AIOKafkaProducer | None = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            linger_ms=5,
            max_batch_size=65536,
            compression_type="gzip",
            acks="all",
        )
        await _producer.start()
    return _producer


async def produce(event: dict) -> None:
    p = await get_producer()
    key = event["chamberId"].encode("utf-8")
    value = json.dumps(event).encode("utf-8")
    await p.send(TOPIC, key=key, value=value)


async def close() -> None:
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None