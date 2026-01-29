
import asyncio
import json
from typing import Any
from contextlib import asynccontextmanager
from fastapi import FastAPI
from faststream.kafka import KafkaBroker

broker = KafkaBroker("redpanda:9092")

@asynccontextmanager
async def lifespan(app: FastAPI):
    for _ in range(10):
        try:
            await broker.start()
            break
        except Exception:
            await asyncio.sleep(2)
    yield
    await broker.close()

app = FastAPI(lifespan=lifespan)


@broker.subscriber("loan.requests")
async def auto_approve(message: Any):
    try:
        if isinstance(message, dict):
            payload = message
        elif isinstance(message, (str, bytes, bytearray)):
            payload = json.loads(message)
        else:
            return
    except Exception:
        return

    loan_id = payload.get("id")
    if loan_id:
        await broker.publish({"id": loan_id, "status": "approved"}, topic="loan.status")

@app.post("/approve/{loan_id}")
async def approve(loan_id: str):
    await broker.publish({"id": loan_id, "status": "approved"}, topic="loan.status")
    return {"status": "approved"}

@app.post("/deny/{loan_id}")
async def deny(loan_id: str):
    await broker.publish({"id": loan_id, "status": "denied"}, topic="loan.status")
    return {"status": "denied"}
