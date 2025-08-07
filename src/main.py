from fastapi import FastAPI, HTTPException
from loguru import logger
from pydantic import BaseModel
from datetime import datetime
from typing import Literal
from dataclasses import dataclass
import asyncio
import json

from .db import EventDB

app = FastAPI(title="Browsing Event API")

QUEUE_MAXSIZE = 1000
WORKER_COUNT = 1
QUEUE_JOIN_TIMEOUT_SEC = 5.0


class BrowsingEvent(BaseModel):
    type: str
    url: str
    title: str
    timestamp: datetime
    user: str | None = None


class EmacsContext(BaseModel):
    buffer: str
    file_name: str | None = None
    major_mode: str
    project: str | None = None


class EmacsEvent(BaseModel):
    timestamp: datetime
    session_id: str
    host: str
    command: str
    context: EmacsContext


@dataclass
class QueueItem:
    kind: Literal["chrome", "emacs"]
    payload: dict


async def event_worker(name: str, queue: asyncio.Queue):
    """
    Background worker that consumes events and stores them in the DB.
    """
    db = EventDB()
    logger.info(f"[{name}] worker started")
    try:
        while True:
            item: QueueItem = await queue.get()
            try:
                if item.kind == "chrome":
                    event_id = db.store_browsing_event(item.payload)
                    logger.info(
                        f"[{name}] Stored event {event_id} (chrome): "
                        f"{json.dumps(item.payload, indent=2, default=str)}"
                    )
                elif item.kind == "emacs":
                    event_id = db.store_emacs_event(item.payload)
                    logger.info(
                        f"[{name}] Stored event {event_id} (emacs): "
                        f"{json.dumps(item.payload, indent=2, default=str)}"
                    )
                else:
                    logger.error(f"[{name}] Unknown event kind: {item.kind}")
            except Exception:
                logger.exception(f"[{name}] Failed to store {item.kind} event")
            finally:
                queue.task_done()
    except asyncio.CancelledError:
        logger.info(f"[{name}] worker cancelled, exiting")
        raise
    finally:
        try:
            close = getattr(db, "close", None)
            if callable(close):
                close()
        except Exception:
            logger.exception(f"[{name}] Error during DB close")


@app.on_event("startup")
async def startup_event():
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    app.state.event_queue = queue
    app.state.event_workers = [
        asyncio.create_task(event_worker(f"worker-{i+1}", queue))
        for i in range(WORKER_COUNT)
    ]
    logger.info(
        f"Initialized event queue (maxsize={QUEUE_MAXSIZE}) with {WORKER_COUNT} worker(s)"
    )


@app.on_event("shutdown")
async def shutdown_event():
    queue: asyncio.Queue = app.state.event_queue
    try:
        await asyncio.wait_for(queue.join(), timeout=QUEUE_JOIN_TIMEOUT_SEC)
        logger.info("Event queue drained")
    except asyncio.TimeoutError:
        logger.warning("Event queue drain timed out; cancelling workers")

    for task in app.state.event_workers:
        task.cancel()
    await asyncio.gather(*app.state.event_workers, return_exceptions=True)
    logger.info("All workers stopped")


@app.post("/chrome-events")
async def receive_chrome_event(event: BrowsingEvent):
    """Receive and process browsing event messages"""
    event_dict = event.model_dump()
    try:
        app.state.event_queue.put_nowait(QueueItem(kind="chrome", payload=event_dict))
    except asyncio.QueueFull:
        logger.warning("Event queue full; dropping chrome event")
        raise HTTPException(status_code=503, detail="Event queue is full")

    logger.info(
        f"Enqueued chrome event: {json.dumps(event_dict, indent=2, default=str)}"
    )
    return {"status": "queued"}


@app.post("/emacs-events")
async def receive_emacs_event(event: EmacsEvent):
    """Receive and process Emacs event messages"""
    event_dict = event.model_dump()
    try:
        app.state.event_queue.put_nowait(QueueItem(kind="emacs", payload=event_dict))
    except asyncio.QueueFull:
        logger.warning("Event queue full; dropping emacs event")
        raise HTTPException(status_code=503, detail="Event queue is full")

    logger.info(
        f"Enqueued emacs event: {json.dumps(event_dict, indent=2, default=str)}"
    )
    return {"status": "queued"}
