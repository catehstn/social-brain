from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)


def collect_calendly(
    token: str,
    since: datetime | None = None,
    lead_gen_event: str | None = None,
) -> dict[str, Any] | None:
    """
    Collect booking data from Calendly as a lead-gen metric.
    Returns bookings grouped by event type with active/cancelled counts.
    If lead_gen_event is set, the active count for that event type is
    surfaced separately as lead_gen_bookings.
    Requires a Personal Access Token from calendly.com/integrations/api_webhooks.
    """
    if since is None:
        since = _default_since()

    base = "https://api.calendly.com"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        with httpx.Client(timeout=30, headers=headers) as client:
            # Resolve current user URI
            r = client.get(f"{base}/users/me")
            r.raise_for_status()
            user_uri = r.json()["resource"]["uri"]

            # Fetch event types (to map URIs → friendly names)
            r = client.get(f"{base}/event_types", params={"user": user_uri, "count": 100})
            r.raise_for_status()
            event_types_raw = r.json().get("collection", [])
            event_type_names: dict[str, str] = {
                et["uri"]: et["name"] for et in event_types_raw
            }

            # Fetch scheduled events within the lookback window
            now = _utcnow()
            params = {
                "user": user_uri,
                "min_start_time": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "max_start_time": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "count": 100,
                "status": "active",
            }
            r = client.get(f"{base}/scheduled_events", params=params)
            r.raise_for_status()
            active_events = r.json().get("collection", [])

            params["status"] = "canceled"
            r = client.get(f"{base}/scheduled_events", params=params)
            r.raise_for_status()
            canceled_events = r.json().get("collection", [])

        # If a lead_gen_event is specified, restrict counting to that event type only
        def _event_name(event: dict) -> str:
            et_uri = event.get("event_type", "")
            return event_type_names.get(et_uri, et_uri.split("/")[-1])

        if lead_gen_event:
            active_events = [e for e in active_events if _event_name(e) == lead_gen_event]
            canceled_events = [e for e in canceled_events if _event_name(e) == lead_gen_event]

        # Aggregate by event type
        by_type: dict[str, dict[str, int]] = {}
        for event in active_events:
            name = _event_name(event)
            by_type.setdefault(name, {"active": 0, "canceled": 0})["active"] += 1
        for event in canceled_events:
            name = _event_name(event)
            by_type.setdefault(name, {"active": 0, "canceled": 0})["canceled"] += 1

        bookings_by_type = [
            {"event_type": name, **counts}
            for name, counts in sorted(by_type.items())
        ]
        total_active = sum(e["active"] for e in bookings_by_type)
        total_canceled = sum(e["canceled"] for e in bookings_by_type)

        result: dict[str, Any] = {
            "platform": "calendly",
            "collected_at": _iso(_utcnow()),
            "period_start": _iso(since),
            "period_end": _iso(now),
            "total_bookings": total_active,
            "total_canceled": total_canceled,
            "bookings_by_type": bookings_by_type,
        }

        if lead_gen_event:
            result["lead_gen_event"] = lead_gen_event
            match = next((e for e in bookings_by_type if e["event_type"] == lead_gen_event), None)
            result["lead_gen_bookings"] = match["active"] if match else 0

        return result
    except Exception as exc:
        logger.error("Calendly collection failed: %s", exc)
        return None
