from fastapi import FastAPI
from loguru import logger
from pydantic import BaseModel
from datetime import datetime
import json

from .db import BrowsingEventDB

db = BrowsingEventDB()

app = FastAPI(title="Browsing Event API")


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
    command: str
    context: EmacsContext


@app.post("/chrome-events")
async def receive_chrome_event(event: BrowsingEvent):
    """Receive and process browsing event messages"""
    # logger.info(f"Received event {json.dumps(event, indent=2, default=str)}")
    event_dict = event.model_dump()
    event_id = db.store_event(event_dict)

    logger.info(f"Stored event {event_id}: {json.dumps(event_dict, indent=2, default=str)}")

    return {
        "status": "ok",
    }


@app.post("/emacs-events")
async def receive_emacs_event(event: EmacsEvent):
    """Receive and process Emacs event messages"""
    event_dict = event.model_dump()
    # event_id = db.store_event(event_dict)
    logger.info(f"Received Emacs event {json.dumps(event_dict, indent=2, default=str)}")
    # logger.info(f"Stored Emacs event {event_id}: {json.dumps(event_dict, indent=2, default=str)}")

    return {
        "status": "ok",
    }
