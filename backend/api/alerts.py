from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from backend.settings import Settings
from backend.deps import get_app_settings

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


class AlertTestRequest(BaseModel):
    text: str = "MarketPulse-X Telegram ✅"


@router.post("/test")
async def send_test_alert(
    payload: AlertTestRequest,
    request: Request,
    settings: Settings = Depends(get_app_settings),
):
    notifier = getattr(request.app.state, "notifier", None)
    if notifier is None:
        raise HTTPException(status_code=503, detail="notifier unavailable")
    status = await notifier.send_message(payload.text, dedupe_key="test", cooldown_secs=0)
    return {"status": status}
