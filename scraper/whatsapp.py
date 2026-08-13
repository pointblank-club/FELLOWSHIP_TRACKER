"""WhatsApp alert delivery for Fellowship Tracker opportunities."""
from __future__ import annotations
from datetime import datetime
import httpx

def classify_opportunity_event(
    previous: dict | None,
    *,
    is_new: bool,
    is_open: bool,
) -> str | None:
    """Return the alert event for a newly inserted or newly opened record."""
    if is_new:
        return "new"
    # Only an explicit closed state is a reopen. Missing status on legacy
    # records should not produce a burst of alerts after this integration is
    # enabled for the first time.
    if previous is not None and previous.get("is_open") is False and is_open:
        return "reopened"
    return None

def make_idempotency_key(event: str, apply_link: str, last_updated: datetime) -> str:
    return f"{event}:{apply_link}:{last_updated.isoformat()}"

async def send_whatsapp_alert(
    doc: dict,
    *,
    event: str,
    idempotency_key: str,
    url: str | None,
    secret: str | None,
) -> bool:
    """POST one signed opportunity alert to the bot webhook."""
    if not url or not secret:
        print("WhatsApp alerts disabled: set WHATSAPP_ALERT_URL and WHATSAPP_ALERT_SECRET.")
        return False
    payload = {
        **doc,
        "event": event,
        "idempotency_key": idempotency_key,
    }
    last_updated = payload.get("last_updated")
    if hasattr(last_updated, "isoformat"):
        payload["last_updated"] = last_updated.isoformat()
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                json=payload,
                headers={"X-Fellowship-Alert-Secret": secret},
                timeout=10,
            )
            response.raise_for_status()
        print(f"WhatsApp notified: {doc.get('name')} ({event})")
        return True
    except httpx.HTTPError as exc:
        print(f"WhatsApp alert failed: {exc}")
        return False