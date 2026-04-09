from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)


def collect_goatcounter(
    site: str,
    token: str,
    since: datetime | None = None,
) -> dict[str, Any] | None:
    """
    Collect pageview stats from GoatCounter.
    Returns total pageviews, unique visitors, and raccoon result distribution.
    """
    if since is None:
        since = _default_since()

    base = f"https://{site}.goatcounter.com/api/v0"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    now = _utcnow()
    # API expects date-time rounded to the hour
    start = since.strftime("%Y-%m-%dT00:00:00Z")
    end = now.strftime("%Y-%m-%dT%H:00:00Z")
    # Keep plain dates for the result metadata
    start_date = since.strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    try:
        with httpx.Client(timeout=30, headers=headers) as client:
            r = client.get(f"{base}/stats/total", params={"start": start, "end": end})
            if not r.is_success:
                logger.error("GoatCounter stats/total failed: HTTP %s — %s", r.status_code, r.text[:300])
                return None
            total_data = r.json()

            r = client.get(f"{base}/stats/hits", params={"start": start, "end": end, "limit": 200})
            if not r.is_success:
                logger.error("GoatCounter stats/hits failed: HTTP %s — %s", r.status_code, r.text[:300])
                return None
            hits_data = r.json()

        hits = hits_data.get("hits", [])
        top_paths = [
            {"path": h["path"], "count": h["count"]}
            for h in hits if not h.get("event", False)
        ]
        events = [
            {"event": h["path"], "count": h["count"]}
            for h in hits if h.get("event", False)
        ]

        return {
            "platform": "goatcounter",
            "collected_at": _iso(now),
            "period_start": start_date,
            "period_end": end_date,
            "total_visitors": total_data.get("total", 0),
            "total_events": total_data.get("total_events", 0),
            "top_paths": top_paths,
            "events": events,
        }
    except Exception as exc:
        logger.error("GoatCounter collection failed: %s", exc)
        return None
