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
    start = since.strftime("%Y-%m-%d")
    end = _utcnow().strftime("%Y-%m-%d")

    try:
        with httpx.Client(timeout=30, headers=headers) as client:
            r = client.get(f"{base}/stats/total", params={"start": start, "end": end})
            r.raise_for_status()
            total_data = r.json()

            r = client.get(f"{base}/stats/hits", params={"start": start, "end": end, "limit": 200})
            r.raise_for_status()
            hits_data = r.json()

        hits = hits_data.get("hits", [])
        top_paths = [
            {"path": h["path"], "count": h["count"]}
            for h in hits if h["path"].startswith("/")
        ]
        events = [
            {"event": h["path"], "count": h["count"]}
            for h in hits if not h["path"].startswith("/")
        ]

        return {
            "platform": "goatcounter",
            "collected_at": _iso(_utcnow()),
            "period_start": start,
            "period_end": end,
            "total_pageviews": total_data.get("total", 0),
            "total_unique": total_data.get("total_unique", 0),
            "top_paths": top_paths,
            "events": events,
        }
    except Exception as exc:
        logger.error("GoatCounter collection failed: %s", exc)
        return None
