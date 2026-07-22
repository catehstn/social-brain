from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)

DEFAULT_HOST = "https://us.i.posthog.com"


def _hogql(client: httpx.Client, host: str, project_id: str, query: str) -> list[list] | None:
    """
    Run a HogQL query via POST /api/projects/{id}/query/.
    Returns the ``results`` array (list of rows, each row is a list of values)
    or None on failure.
    """
    url = f"{host.rstrip('/')}/api/projects/{project_id}/query/"
    payload = {"query": {"kind": "HogQLQuery", "query": query}}
    try:
        r = client.post(url, json=payload)
    except httpx.HTTPError as exc:
        logger.error("PostHog HogQL request failed (%s): %s", type(exc).__name__, exc)
        return None
    if not r.is_success:
        logger.error(
            "PostHog HogQL failed: HTTP %s — %s",
            r.status_code, r.text[:300],
        )
        return None
    return r.json().get("results", [])


def collect_posthog(
    api_key: str,
    project_id: str,
    host: str | None = None,
    since: datetime | None = None,
) -> dict[str, Any] | None:
    """
    Collect web analytics from PostHog via HogQL.

    Replaces the retired Vercel Web Analytics collector. The output shape
    mirrors the old collector so downstream (store, analyse, dashboard)
    can consume it via the same keys:

        {
            "platform": "posthog",
            "project": <project_id>,
            "collected_at": ..., "since": ...,
            "page_views": int, "visitors": int,
            "daily": [{"date": "YYYY-MM-DD", "page_views": int, "visitors": int}, ...],
            "top_pages": [{"path": str, "page_views": int, "visitors": int}, ...],
            "top_referrers": [{"referrer": str, "page_views": int}, ...],
        }
    """
    if since is None:
        since = _default_since()

    host = host or DEFAULT_HOST
    now = _utcnow()
    since_iso = since.strftime("%Y-%m-%d %H:%M:%S")
    # HogQL uses ClickHouse date functions; toDateTime() parses the string.
    since_expr = f"toDateTime('{since_iso}')"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # 1. Overview — total page views + unique visitors
    overview_q = f"""
        SELECT count() AS page_views,
               count(DISTINCT distinct_id) AS visitors
        FROM events
        WHERE event = '$pageview' AND timestamp >= {since_expr}
    """

    # 2. Daily timeseries
    daily_q = f"""
        SELECT toDate(timestamp) AS day,
               count() AS page_views,
               count(DISTINCT distinct_id) AS visitors
        FROM events
        WHERE event = '$pageview' AND timestamp >= {since_expr}
        GROUP BY day
        ORDER BY day
    """

    # 3. Top pages by page views (path, views, visitors)
    top_pages_q = f"""
        SELECT properties.$pathname AS path,
               count() AS page_views,
               count(DISTINCT distinct_id) AS visitors
        FROM events
        WHERE event = '$pageview' AND timestamp >= {since_expr}
          AND properties.$pathname IS NOT NULL
        GROUP BY path
        ORDER BY page_views DESC
        LIMIT 50
    """

    # 4. Top referrers by page views (referrer, views)
    top_refs_q = f"""
        SELECT properties.$referring_domain AS referrer,
               count() AS page_views
        FROM events
        WHERE event = '$pageview' AND timestamp >= {since_expr}
          AND properties.$referring_domain IS NOT NULL
          AND properties.$referring_domain != '$direct'
        GROUP BY referrer
        ORDER BY page_views DESC
        LIMIT 20
    """

    with httpx.Client(timeout=30, headers=headers) as client:
        overview = _hogql(client, host, project_id, overview_q)
        if overview is None:
            logger.error("PostHog: overview query failed — aborting collection")
            return None
        daily_rows = _hogql(client, host, project_id, daily_q) or []
        top_pages_rows = _hogql(client, host, project_id, top_pages_q) or []
        top_refs_rows = _hogql(client, host, project_id, top_refs_q) or []

    if overview:
        page_views = int(overview[0][0] or 0)
        visitors = int(overview[0][1] or 0)
    else:
        page_views = 0
        visitors = 0

    def _row_date(v: Any) -> str:
        # HogQL date columns come back as ISO date strings or datetime.date;
        # be defensive.
        if isinstance(v, str):
            return v[:10]
        try:
            return v.strftime("%Y-%m-%d")
        except AttributeError:
            return str(v)[:10]

    daily = [
        {
            "date": _row_date(row[0]),
            "page_views": int(row[1] or 0),
            "visitors": int(row[2] or 0),
        }
        for row in daily_rows
    ]

    top_pages = [
        {
            "path": row[0] or "",
            "page_views": int(row[1] or 0),
            "visitors": int(row[2] or 0),
        }
        for row in top_pages_rows
    ]

    top_referrers = [
        {"referrer": row[0] or "", "page_views": int(row[1] or 0)}
        for row in top_refs_rows
    ]

    logger.info(
        "PostHog: %s page views, %s visitors since %s",
        page_views, visitors, _iso(since),
    )

    return {
        "platform": "posthog",
        "project": project_id,
        "collected_at": _iso(now),
        "since": _iso(since),
        "page_views": page_views,
        "visitors": visitors,
        "daily": daily,
        "top_pages": top_pages,
        "top_referrers": top_referrers,
    }
