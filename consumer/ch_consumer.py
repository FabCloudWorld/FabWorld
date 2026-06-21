import asyncio
import json
import os
import time

import httpx
from aiokafka import AIOKafkaConsumer
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CH_DB   = os.getenv("CLICKHOUSE_DB",   "fabworld")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASS", "")

CH_URL = f"http://{CH_HOST}:{CH_PORT}/"

BATCH_SIZE    = 2000
FLUSH_EVERY_S = 2.0

_RAW_SQL = (
    f"INSERT INTO {CH_DB}.sensor_raw "
    "(equipmentId,chamberId,sensorType,value,timestamp,waferId,lotId,recipeId,isAnomaly) "
    "FORMAT JSONEachRow"
)
_ANOMALY_SQL = (
    f"INSERT INTO {CH_DB}.anomaly_events "
    "(equipmentId,chamberId,sensorType,value,timestamp,waferId,lotId,recipeId,"
    "isAnomaly,anomalyType,severity,reason,detectedAt) "
    "FORMAT JSONEachRow"
)


async def _insert(client: httpx.AsyncClient, sql: str, rows: list[dict]) -> None:
    body = "\n".join(json.dumps(r) for r in rows).encode()
    resp = await client.post(
        CH_URL,
        params={"query": sql, "user": CH_USER, "password": CH_PASS},
        content=body,
        timeout=10,
    )
    if resp.status_code != 200:
        print(f"[consumer] CH error {resp.status_code}: {resp.text[:200]}")


async def _flush(client: httpx.AsyncClient, raw: list, anomaly: list) -> None:
    tasks = []
    if raw:
        tasks.append(_insert(client, _RAW_SQL, raw))
    if anomaly:
        tasks.append(_insert(client, _ANOMALY_SQL, anomaly))
    if tasks:
        await asyncio.gather(*tasks)
        print(f"[consumer] flushed raw={len(raw)} anomaly={len(anomaly)}")


async def main() -> None:
    consumer = AIOKafkaConsumer(
        "sensor-raw", "anomaly-events",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="fab-ch-consumer",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print("[consumer] started → ClickHouse")

    raw_buf: list[dict]     = []
    anomaly_buf: list[dict] = []
    last_flush = time.monotonic()

    async with httpx.AsyncClient() as client:
        try:
            async for msg in consumer:
                ev = msg.value
                if msg.topic == "sensor-raw":
                    raw_buf.append(ev)
                else:
                    anomaly_buf.append(ev)

                now = time.monotonic()
                if len(raw_buf) + len(anomaly_buf) >= BATCH_SIZE or now - last_flush >= FLUSH_EVERY_S:
                    await _flush(client, raw_buf, anomaly_buf)
                    raw_buf.clear()
                    anomaly_buf.clear()
                    last_flush = now
        finally:
            if raw_buf or anomaly_buf:
                await _flush(client, raw_buf, anomaly_buf)
            await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())