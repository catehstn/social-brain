from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)


def collect_jetpack(
    site: str,
    access_token: str,
    since: datetime | None = None,
) -> dict[str, Any] | None:
    """
    Collect daily page views and top posts from Jetpack Stats back to `since`
    (default: 2 weeks ago).
    """
    if since is None:
        since = _default_since()

    try:
        headers = {"Authorization": f"Bearer {access_token}"}
        base = f"https://public-api.wordpress.com/rest/v1.1/sites/{site}/stats"

        today = _utcnow().date()
        quantity = (today - since.date()).days + 1

        with httpx.Client(timeout=30, headers=headers) as client:
            r = client.get(
                f"{base}/visits",
                params={
                    "unit": "day",
                    "quantity": quantity,
                    "date": today.isoformat(),
                },
            )
            r.raise_for_status()
            visits_data = r.json()

            # For longer periods use "month" period for top posts to get a broader view
            top_posts_period = "month" if quantity > 30 else "week"
            tp = client.get(
                f"{base}/top-posts",
                params={
                    "period": top_posts_period,
                    "date": today.isoformat(),
                    "num": 10,
                    "max": 10,
                },
            )
            tp.raise_for_status()
            top_posts_data = tp.json()

            rr = client.get(
                f"{base}/referrers",
                params={
                    "period": "day",
                    "num": quantity,
                    "date": today.isoformat(),
                },
            )
            rr.raise_for_status()
            referrers_data = rr.json()

        daily_views = []
        for row in visits_data.get("data", []):
            if isinstance(row, list) and len(row) >= 2:
                daily_views.append({"date": row[0], "views": row[1]})

        # The API returns either a flat "top-posts" list or a "days" dict
        # with per-day postviews arrays — handle both and aggregate.
        top_posts_raw: dict[str, dict] = {}

        if "top-posts" in top_posts_data:
            for post in top_posts_data["top-posts"]:
                href = post.get("href", "")
                top_posts_raw[href] = {
                    "title": post.get("title", ""),
                    "href": href,
                    "views": post.get("views", 0),
                }
        elif "days" in top_posts_data:
            for day_data in top_posts_data["days"].values():
                for post in day_data.get("postviews", []):
                    href = post.get("href", "")
                    if not href:
                        continue
                    if href in top_posts_raw:
                        top_posts_raw[href]["views"] += post.get("views", 0)
                    else:
                        top_posts_raw[href] = {
                            "title": post.get("title", ""),
                            "href": href,
                            "views": post.get("views", 0),
                        }

        top_posts = sorted(
            top_posts_raw.values(), key=lambda p: p["views"], reverse=True
        )[:10]

        referrers_raw: dict[str, int] = {}
        for day_data in referrers_data.get("days", {}).values():
            for group in day_data.get("groups", []):
                name = group.get("name") or group.get("group", "")
                referrers_raw[name] = referrers_raw.get(name, 0) + group.get("total", 0)
        referrers = sorted(
            [{"name": k, "views": v} for k, v in referrers_raw.items()],
            key=lambda x: x["views"],
            reverse=True,
        )[:20]

        total_views = sum(d["views"] for d in daily_views)
        logger.info(
            "Jetpack: collected %d days of data (%d total views, %d referrers)",
            len(daily_views),
            total_views,
            len(referrers),
        )
        return {
            "platform": "jetpack",
            "site": site,
            "collected_at": _iso(_utcnow()),
            "since": _iso(since),
            "total_views": total_views,
            "daily_views": daily_views,
            "top_posts": top_posts,
            "referrers": referrers,
        }

    except Exception as exc:
        logger.error("Jetpack collection failed: %s", exc)
        return None
