import time
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import kafka_producer
from schema import SensorEvent

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await kafka_producer.close()


app = FastAPI(title="FabWorld Producer", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST"],
    allow_headers=["*"],
)


@app.post("/events", status_code=202)
async def receive_event(event: SensorEvent):
    payload = event.model_dump()
    if not payload["timestamp"]:
        payload["timestamp"] = time.time_ns() // 1_000_000
    await kafka_producer.produce(payload)
    return {"status": "ok"}


@app.get("/health")
async def health():
    return {"status": "ok"}