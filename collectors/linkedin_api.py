"""
LinkedIn DMA API collector (EU/EEA/Switzerland only).

Uses the LinkedIn Member Data Portability API (r_dma_portability_self_serve scope)
to fetch post content and per-post analytics without a manual CSV export.

OAuth flow is handled separately by `python run.py --auth linkedin`.
Tokens are valid 60 days; no refresh token is issued.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)

_BASE = "https://api.linkedin.com"
_API_VERSION = "202312"
_HEADERS_BASE = {
    "LinkedIn-Version": _API_VERSION,
    "X-Restli-Protocol-Version": "2.0.0",
}


def _auth_headers(access_token: str) -> dict[str, str]:
    return {**_HEADERS_BASE, "Authorization": f"Bearer {access_token}"}


def _get(client: httpx.Client, url: str, **kwargs: Any) -> httpx.Response:
    r = client.get(url, **kwargs)
    r.raise_for_status()
    return r


def _fetch_posts(client: httpx.Client) -> list[dict[str, Any]]:
    """
    Fetch the authenticated member's posts via memberSnapshotData.
    Returns a list of raw post dicts.
    """
    posts: list[dict[str, Any]] = []
    start = 0
    count = 50

    while True:
        r = _get(
            client,
            f"{_BASE}/rest/memberSnapshotData",
            params={
                "q": "criteria",
                "domain": "MEMBER_SHARE_INFO",
                "start": start,
                "count": count,
            },
        )
        data = r.json()
        elements = data.get("elements", [])
        if not elements:
            break
        posts.extend(elements)
        if len(elements) < count:
            break
        start += count

    return posts


def _fetch_post_analytics(client: httpx.Client, post_urn: str) -> dict[str, Any]:
    """
    Fetch per-post analytics for a single post URN.
    Returns a dict of metric name → value, or {} on failure.
    """
    try:
        r = _get(
            client,
            f"{_BASE}/rest/memberCreatorPostAnalytics",
            params={"q": "entity", "entity": post_urn},
        )
        data = r.json()
        elements = data.get("elements", [])
        if not elements:
            return {}
        # Each element has a "totalCountsByAction" or similar structure depending on API version
        # The DMA API returns a list of metric objects
        metrics: dict[str, Any] = {}
        for el in elements:
            metric_type = el.get("type") or el.get("metricType", "")
            value = el.get("value") or el.get("count", 0)
            if metric_type:
                metrics[metric_type.lower()] = value
        return metrics
    except Exception as exc:
        logger.debug("Analytics fetch failed for %s: %s", post_urn, exc)
        return {}


def _fetch_changelogs(client: httpx.Client) -> list[dict[str, Any]]:
    """
    Fetch recent member change-log events (last 28 days).
    Returns a list of event dicts.
    """
    try:
        r = _get(
            client,
            f"{_BASE}/rest/memberChangeLogs",
            params={"q": "memberAndApplication"},
        )
        return r.json().get("elements", [])
    except Exception as exc:
        logger.debug("Change-log fetch failed: %s", exc)
        return []


def _parse_post_date(raw_post: dict[str, Any]) -> datetime | None:
    """Extract publication datetime from a raw post element."""
    # The DMA API nests the share under snapshotData
    snapshot = raw_post.get("snapshotData", raw_post)
    created_ms = (
        snapshot.get("created", {}).get("time")
        or snapshot.get("firstPublishedAt")
    )
    if created_ms and isinstance(created_ms, int):
        return datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
    # ISO string fallback
    date_str = snapshot.get("firstPublishedAt") or snapshot.get("created")
    if isinstance(date_str, str):
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except ValueError:
            pass
    return None


def _parse_post_text(raw_post: dict[str, Any]) -> str:
    """Extract post text from a raw post element."""
    snapshot = raw_post.get("snapshotData", raw_post)
    # Commentary field (text of the share)
    commentary = (
        snapshot.get("commentary")
        or snapshot.get("specificContent", {})
            .get("com.linkedin.ugc.ShareContent", {})
            .get("shareCommentary", {})
            .get("text", "")
    )
    return str(commentary).strip()


def _parse_post_urn(raw_post: dict[str, Any]) -> str | None:
    """Extract the post URN for analytics lookup."""
    return (
        raw_post.get("urn")
        or raw_post.get("snapshotData", {}).get("urn")
    )


def _parse_post_url(raw_post: dict[str, Any]) -> str | None:
    """Extract a canonical post URL from a raw post element."""
    snapshot = raw_post.get("snapshotData", raw_post)
    return snapshot.get("permalink") or snapshot.get("url")


def collect_linkedin_api(
    access_token: str,
    since: datetime | None = None,
) -> dict[str, Any] | None:
    """
    Collect LinkedIn post analytics via the EU DMA portability API.

    Args:
        access_token: OAuth bearer token (r_dma_portability_self_serve scope).
        since: Only include posts published after this datetime.
               Defaults to 2 weeks ago.

    Returns:
        Collected data dict, or None on fatal error.
    """
    if not access_token:
        logger.error("LinkedIn API: no access_token — run `python run.py --auth linkedin` first")
        return None

    if since is None:
        since = _default_since()

    try:
        with httpx.Client(headers=_auth_headers(access_token), timeout=30) as client:
            # 1. Fetch posts
            try:
                raw_posts = _fetch_posts(client)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 401:
                    logger.error(
                        "LinkedIn API: token expired or invalid (401) — "
                        "run `python run.py --auth linkedin` to get a new token"
                    )
                elif exc.response.status_code == 403:
                    logger.error(
                        "LinkedIn API: permission denied (403) — "
                        "ensure your app has r_dma_portability_self_serve scope "
                        "and is approved for DMA access"
                    )
                else:
                    logger.error("LinkedIn API: posts fetch failed (%s)", exc.response.status_code)
                return None
            except Exception as exc:
                logger.error("LinkedIn API: posts fetch failed: %s", exc)
                return None

            logger.info("LinkedIn API: fetched %d raw posts", len(raw_posts))

            # 2. Filter by date and fetch per-post analytics
            posts: list[dict[str, Any]] = []
            for raw in raw_posts:
                pub_dt = _parse_post_date(raw)
                if pub_dt and pub_dt < since:
                    continue

                urn = _parse_post_urn(raw)
                analytics = _fetch_post_analytics(client, urn) if urn else {}

                post: dict[str, Any] = {
                    "urn": urn,
                    "text": _parse_post_text(raw),
                    "url": _parse_post_url(raw),
                    "published_at": _iso(pub_dt) if pub_dt else None,
                    # Normalise metric names to match CSV-drop output
                    "impressions": analytics.get("impression") or analytics.get("impressions", 0),
                    "reactions": analytics.get("reaction") or analytics.get("reactions", 0),
                    "comments": analytics.get("comment") or analytics.get("comments", 0),
                    "shares": analytics.get("reshare") or analytics.get("shares", 0),
                    "clicks": analytics.get("link_click") or analytics.get("link_clicks") or analytics.get("clicks", 0),
                    "members_reached": analytics.get("members_reached", 0),
                }
                posts.append(post)

            # 3. Fetch change-log events
            changelogs = _fetch_changelogs(client)

            logger.info(
                "LinkedIn API: %d post(s) in window, %d change-log event(s)",
                len(posts),
                len(changelogs),
            )

            total_impressions = sum(p.get("impressions") or 0 for p in posts)
            total_reactions = sum(p.get("reactions") or 0 for p in posts)
            total_comments = sum(p.get("comments") or 0 for p in posts)
            total_shares = sum(p.get("shares") or 0 for p in posts)
            total_clicks = sum(p.get("clicks") or 0 for p in posts)

            return {
                "platform": "linkedin",
                "source": "api",
                "collected_at": _iso(_utcnow()),
                "period_start": _iso(since),
                "posts": posts,
                "summary": {
                    "total_impressions": total_impressions,
                    "total_reactions": total_reactions,
                    "total_comments": total_comments,
                    "total_shares": total_shares,
                    "total_clicks": total_clicks,
                    "post_count": len(posts),
                },
                "changelogs": changelogs,
            }

    except Exception as exc:
        logger.error("LinkedIn API: unexpected error: %s", exc)
        return None
