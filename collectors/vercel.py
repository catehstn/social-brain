from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)


def collect_vercel(
    token: str,
    project_slug: str,
    team_id: str | None = None,
    since: datetime | None = None,
) -> dict[str, Any] | None:
    """
    Collect web analytics from Vercel's internal analytics API.
    Fetches overview stats, daily timeseries, top pages, and referrers.
    Uses the same endpoints as the Vercel dashboard.
    Note: undocumented internal API — may change without notice.
    """
    if since is None:
        since = _default_since()

    try:
        now = _utcnow()
        base = "https://vercel.com/api/web-analytics"
        headers = {"Authorization": f"Bearer {token}"}
        common_params: dict[str, Any] = {
            "projectId": project_slug,
            "environment": "production",
            "filter": "{}",
            "from": since.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "to": now.strftime("%Y-%m-%dT%H:%M:%S.999Z"),
            "tz": "UTC",
        }
        if team_id:
            common_params["teamId"] = team_id

        with httpx.Client(timeout=30, headers=headers) as client:
            overview_r = client.get(f"{base}/overview", params=common_params)
            overview_r.raise_for_status()
            overview = overview_r.json()

            ts_r = client.get(f"{base}/timeseries", params=common_params)
            ts_r.raise_for_status()
            timeseries = ts_r.json()

            pages_r = client.get(f"{base}/stats", params={**common_params, "type": "path", "limit": 50})
            pages_r.raise_for_status()
            pages_data = pages_r.json()

            ref_r = client.get(f"{base}/stats", params={**common_params, "type": "referrer", "limit": 10})
            ref_r.raise_for_status()
            referrers_data = ref_r.json()

        # overview: {"total": N, "devices": N, "bounceRate": N}
        page_views = overview.get("total")
        visitors = overview.get("devices")
        bounce_rate = overview.get("bounceRate")

        # timeseries: {"data": {"groups": {"all": [{"key": "YYYY-MM-DD", "total": N, "devices": N}, ...]}}}
        daily = []
        for entry in timeseries.get("data", {}).get("groups", {}).get("all", []):
            daily.append({
                "date": entry.get("key"),
                "page_views": entry.get("total"),
                "visitors": entry.get("devices"),
            })

        # stats: {"data": [{"key": "/path", "total": N, "devices": N}, ...]}
        top_pages = [
            {"path": p.get("key", ""), "page_views": p.get("total", 0), "visitors": p.get("devices")}
            for p in pages_data.get("data", [])
        ]
        top_referrers = [
            {"referrer": r.get("key", ""), "page_views": r.get("total", 0)}
            for r in referrers_data.get("data", [])
        ]

        logger.info(
            "Vercel: %s page views, %s visitors since %s",
            page_views, visitors, _iso(since),
        )
        return {
            "platform": "vercel",
            "project": project_slug,
            "collected_at": _iso(now),
            "since": _iso(since),
            "page_views": page_views,
            "visitors": visitors,
            "bounce_rate_pct": bounce_rate,
            "daily": daily,
            "top_pages": top_pages,
            "top_referrers": top_referrers,
        }

    except Exception as exc:
        logger.error("Vercel collection failed: %s", exc)
        return None
