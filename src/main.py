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
    tabId: int
    timestamp: datetime
    user: str | None = None


@app.post("/events")
async def receive_event(event: BrowsingEvent):
    """Receive and process browsing event messages"""
    event_dict = event.model_dump()
    event_id = db.store_event(event_dict)

    logger.info(f"Stored event {event_id}: {json.dumps(event_dict, indent=2, default=str)}")

    return {
        "status": "ok",
    }
