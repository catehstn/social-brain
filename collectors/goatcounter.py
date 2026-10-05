from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)

# Backoff between the first attempt and a single retry on transient failures
# (timeouts, 429, 5xx). Module-level so tests can shrink it.
_RETRY_BACKOFF_SECONDS = 2.0


def _get_with_retry(client: httpx.Client, url: str, params: dict) -> httpx.Response:
    """
    GET with one retry on transient failures. Retries only timeouts,
    other httpx transport errors, HTTP 429, and 5xx responses. Auth/other
    4xx errors are returned immediately.
    """
    label = url.rsplit("/", 1)[-1]
    for attempt in range(2):
        try:
            r = client.get(url, params=params)
        except (httpx.TimeoutException, httpx.HTTPError) as exc:
            if attempt == 0:
                logger.warning(
                    "GoatCounter %s: %s — retrying once",
                    label, type(exc).__name__,
                )
                time.sleep(_RETRY_BACKOFF_SECONDS)
                continue
            raise
        if r.is_success:
            return r
        if attempt == 0 and (r.status_code == 429 or r.status_code >= 500):
            logger.warning(
                "GoatCounter %s → HTTP %s — retrying once",
                label, r.status_code,
            )
            time.sleep(_RETRY_BACKOFF_SECONDS)
            continue
        return r
    return r


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
            r = _get_with_retry(client, f"{base}/stats/total", {"start": start, "end": end})
            if not r.is_success:
                logger.error(
                    "GoatCounter stats/total failed: HTTP %s — %s",
                    r.status_code, r.text[:300],
                )
                return None
            total_data = r.json()

            r = _get_with_retry(client, f"{base}/stats/hits", {"start": start, "end": end, "limit": 200})
            if not r.is_success:
                logger.error(
                    "GoatCounter stats/hits failed: HTTP %s — %s",
                    r.status_code, r.text[:300],
                )
                return None
            hits_data = r.json()

        hits = hits_data.get("hits") or []
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
    except httpx.TimeoutException as exc:
        logger.error("GoatCounter collection timed out (%s): %s", type(exc).__name__, exc)
        return None
    except httpx.HTTPError as exc:
        logger.error("GoatCounter HTTP error (%s): %s", type(exc).__name__, exc)
        return None
    except Exception as exc:
        logger.error("GoatCounter collection failed (%s): %s", type(exc).__name__, exc)
        return None
