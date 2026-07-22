from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Iterable

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)

DEFAULT_HOST = "https://us.i.posthog.com"

# Hostname validation for host filter. Rejects anything with quotes / SQL-like
# characters so the filter can be safely inlined into a HogQL string.
_HOSTNAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9.\-:]*$")


def _hogql(client: httpx.Client, host: str, project_id: str, query: str) -> list[list] | None:
    """
    Run a HogQL query via POST /api/projects/{id}/query/.
    Returns the ``results`` array (list of rows, each row is a list of values)
    or None on failure.

    Defensive against: connection errors, non-2xx responses, non-JSON bodies
    (proxy HTML error pages), and top-level payloads that aren't dicts.
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
    try:
        payload = r.json()
    except (ValueError, TypeError) as exc:
        logger.error("PostHog HogQL: non-JSON response body (%s): %s", type(exc).__name__, r.text[:200])
        return None
    if not isinstance(payload, dict):
        logger.error("PostHog HogQL: unexpected response shape (top-level is %s, not dict)", type(payload).__name__)
        return None
    results = payload.get("results")
    return results if isinstance(results, list) else []


def _normalise_host_filter(host_filter: str | Iterable[str] | None) -> list[str]:
    """
    Normalise the caller's host_filter into a list of validated hostnames.
    Rejects anything that isn't a plain domain (letters/digits/dots/dashes/colon
    for optional port) to keep the value safe for HogQL string interpolation.
    """
    if host_filter is None:
        return []
    if isinstance(host_filter, str):
        candidates = [host_filter]
    else:
        candidates = [str(h) for h in host_filter]
    ok: list[str] = []
    for h in candidates:
        h = h.strip()
        if not h:
            continue
        if not _HOSTNAME_RE.match(h):
            logger.warning("PostHog: ignoring host_filter entry %r — invalid hostname", h)
            continue
        ok.append(h)
    return ok


def _host_clause(hosts: list[str]) -> str:
    """Build the AND clause for restricting queries to specific $host values."""
    if not hosts:
        return ""
    if len(hosts) == 1:
        return f"AND properties.$host = '{hosts[0]}'"
    quoted = ", ".join(f"'{h}'" for h in hosts)
    return f"AND properties.$host IN ({quoted})"


def _safe_row(row: Any, expected_len: int) -> list | None:
    """Guard against malformed rows (schema drift, error rows)."""
    if not isinstance(row, (list, tuple)) or len(row) < expected_len:
        return None
    return list(row)


def collect_posthog(
    api_key: str,
    project_id: str,
    host: str | None = None,
    since: datetime | None = None,
    host_filter: str | Iterable[str] | None = None,
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
            "host_filter": [str, ...],   # empty list if no filter applied
            "page_views": int, "visitors": int,
            "daily": [{"date": "YYYY-MM-DD", "page_views": int, "visitors": int}, ...],
            "top_pages": [{"path": str, "page_views": int, "visitors": int}, ...],
            "top_referrers": [{"referrer": str, "page_views": int}, ...],
        }

    Args:
      host_filter: hostname or iterable of hostnames to restrict counting
        to production traffic (default None = count every $host, which
        includes staging / preview deploys sharing the same PostHog
        project). Strongly recommended for any site with preview builds.
        When None and multiple hosts are seen, a warning is logged.
    """
    if since is None:
        since = _default_since()

    host = host or DEFAULT_HOST
    filters = _normalise_host_filter(host_filter)
    host_clause = _host_clause(filters)
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
        {host_clause}
    """

    # 2. Daily timeseries
    daily_q = f"""
        SELECT toDate(timestamp) AS day,
               count() AS page_views,
               count(DISTINCT distinct_id) AS visitors
        FROM events
        WHERE event = '$pageview' AND timestamp >= {since_expr}
        {host_clause}
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
        {host_clause}
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
        {host_clause}
        GROUP BY referrer
        ORDER BY page_views DESC
        LIMIT 20
    """

    # 5. When no filter is set, sample distinct hosts so we can warn if
    #    preview/staging traffic is contaminating the counts.
    hosts_q = f"""
        SELECT properties.$host AS h, count() AS n
        FROM events
        WHERE event = '$pageview' AND timestamp >= {since_expr}
        GROUP BY h ORDER BY n DESC LIMIT 10
    """

    with httpx.Client(timeout=30, headers=headers) as client:
        overview = _hogql(client, host, project_id, overview_q)
        if overview is None:
            logger.error("PostHog: overview query failed — aborting collection")
            return None
        daily_rows = _hogql(client, host, project_id, daily_q) or []
        top_pages_rows = _hogql(client, host, project_id, top_pages_q) or []
        top_refs_rows = _hogql(client, host, project_id, top_refs_q) or []
        if not filters:
            hosts_rows = _hogql(client, host, project_id, hosts_q) or []
            distinct = [h for h in hosts_rows if h and h[0]]
            if len(distinct) > 1:
                summary = ", ".join(f"{r[0]} ({r[1]})" for r in distinct)
                logger.warning(
                    "PostHog: no host_filter set and %d distinct $host values seen — "
                    "counts include preview/staging traffic. Hosts: %s",
                    len(distinct), summary,
                )

    if overview and _safe_row(overview[0], 2):
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

    daily = []
    for row in daily_rows:
        r = _safe_row(row, 3)
        if r is None:
            continue
        daily.append({
            "date": _row_date(r[0]),
            "page_views": int(r[1] or 0),
            "visitors": int(r[2] or 0),
        })

    top_pages = []
    for row in top_pages_rows:
        r = _safe_row(row, 3)
        if r is None:
            continue
        top_pages.append({
            "path": r[0] or "",
            "page_views": int(r[1] or 0),
            "visitors": int(r[2] or 0),
        })

    top_referrers = []
    for row in top_refs_rows:
        r = _safe_row(row, 2)
        if r is None:
            continue
        top_referrers.append({
            "referrer": r[0] or "",
            "page_views": int(r[1] or 0),
        })

    logger.info(
        "PostHog: %s page views, %s visitors since %s%s",
        page_views, visitors, _iso(since),
        f" (host_filter: {', '.join(filters)})" if filters else "",
    )

    return {
        "platform": "posthog",
        "project": project_id,
        "collected_at": _iso(now),
        "since": _iso(since),
        "host_filter": filters,
        "page_views": page_views,
        "visitors": visitors,
        "daily": daily,
        "top_pages": top_pages,
        "top_referrers": top_referrers,
    }
