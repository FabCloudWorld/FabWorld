import argparse
import asyncio
import json
import os
import random
import time

from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "sensor-raw"

SENSOR_TYPES = ["temp", "pressure", "rf_power", "gas_flow", "bias_voltage"]
# 대시보드 $chamberId / seed_data.py 와 동일한 정규 챔버 목록
CHAMBERS = [
    "CVD-A1", "CVD-A2", "PVD-B1", "PVD-B2", "ETCH-C1",
    "ETCH-C2", "INSP-D1", "INSP-D2", "CMP-E1", "CMP-E2",
]
NORMAL_RANGES = {
    "temp":         (280.0, 350.0),
    "pressure":     (1.8,   2.5),
    "rf_power":     (800.0, 1500.0),
    "gas_flow":     (50.0,  200.0),
    "bias_voltage": (100.0, 400.0),
}


def make_event(chamber: str) -> dict:
    sensor = random.choice(SENSOR_TYPES)
    lo, hi = NORMAL_RANGES[sensor]
    return {
        "equipmentId": f"EQ-{chamber}",
        "chamberId":   chamber,
        "sensorType":  sensor,
        "value":       round(random.uniform(lo, hi), 2),
        "timestamp":   time.time_ns() // 1_000_000,
        "waferId":     f"W-{random.randint(1000, 9999)}",
        "lotId":       f"L-{random.randint(1000, 9999)}",
        "recipeId":    "RCP-CVD-001",
        "isAnomaly":   False,
    }


async def run(rate: int) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        linger_ms=5,
        max_batch_size=65536,
        compression_type="gzip",
        acks=1,
    )
    await producer.start()
    print(f"[burst] target={rate} events/sec  topic={TOPIC}")

    sent = 0
    start = time.monotonic()
    interval = 1.0 / rate

    try:
        while True:
            chamber = random.choice(CHAMBERS)
            ev = make_event(chamber)
            await producer.send(
                TOPIC,
                key=ev["chamberId"].encode(),
                value=json.dumps(ev).encode(),
            )
            sent += 1

            if sent % 10_000 == 0:
                elapsed = time.monotonic() - start
                print(f"[burst] {sent / elapsed:,.0f} events/sec  total={sent:,}")

            sleep_for = (sent * interval) - (time.monotonic() - start)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
    finally:
        await producer.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=5000, help="events/sec")
    args = parser.parse_args()
    asyncio.run(run(args.rate))