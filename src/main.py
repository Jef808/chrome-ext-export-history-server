from fastapi import FastAPI
from loguru import logger
from pydantic import BaseModel, HttpUrl
from datetime import datetime
import json

app = FastAPI(title="Browsing Event API")

class BrowsingEvent(BaseModel):
    type: str
    url: HttpUrl
    tabId: int
    timestamp: datetime
    frameId: int | None = None


@app.post("/events")
async def receive_event(event: BrowsingEvent):
    """Receive and process browsing event messages"""
    event_dict = event.model_dump()
    logger.info(f"Received event: {json.dumps(event_dict, indent=2, default=str)}")

    return {
        "status": "ok",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
