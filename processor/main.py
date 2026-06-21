import asyncio
import json
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pathlib import Path

from dotenv import load_dotenv

from alerter import send_slack
from dedup import DedupFilter
from detector import AnomalyDetector

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW     = "sensor-raw"
TOPIC_ANOMALY = "anomaly-events"
GROUP_ID      = "fab-processor"

detector = AnomalyDetector()
dedup    = DedupFilter()


async def main() -> None:
    consumer = AIOKafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="latest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
    )

    await consumer.start()
    await producer.start()
    print(f"[processor] consuming {TOPIC_RAW} ...")

    try:
        async for msg in consumer:
            event = msg.value
            anomaly = detector.check(event)
            if anomaly is None:
                continue

            await producer.send(TOPIC_ANOMALY, key=anomaly["chamberId"], value=anomaly)
            print(f"[anomaly] {anomaly['chamberId']} {anomaly['anomalyType']} {anomaly['severity']}  {anomaly['reason']}")

            if dedup.should_send(anomaly):
                await send_slack(anomaly)
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())