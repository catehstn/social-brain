"""
collect.py — data collectors for each social/content platform.

Each collector returns a dict of structured data on success,
or None on failure (errors are logged but not re-raised so
a single platform outage never kills the whole run).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _cutoff(weeks: int = 2) -> datetime:
    return _utcnow() - timedelta(weeks=weeks)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Mastodon
# ---------------------------------------------------------------------------

def collect_mastodon(instance: str, handle: str) -> dict[str, Any] | None:
    """
    Collect the last 2 weeks of public posts for a Mastodon account.
    Uses the public API — no authentication required.
    """
    try:
        base = f"https://{instance}"
        # 1. Resolve handle → account id
        with httpx.Client(timeout=30) as client:
            r = client.get(
                f"{base}/api/v1/accounts/lookup",
                params={"acct": handle},
            )
            r.raise_for_status()
            account = r.json()
            account_id = account["id"]

        cutoff = _cutoff(weeks=2)
        posts: list[dict] = []

        with httpx.Client(timeout=30) as client:
            params: dict[str, Any] = {
                "limit": 40,
                "exclude_replies": False,
                "exclude_reblogs": True,
            }
            while True:
                r = client.get(
                    f"{base}/api/v1/accounts/{account_id}/statuses",
                    params=params,
                )
                r.raise_for_status()
                batch = r.json()
                if not batch:
                    break

                for post in batch:
                    created = datetime.fromisoformat(
                        post["created_at"].replace("Z", "+00:00")
                    )
                    if created < cutoff:
                        batch = []  # signal outer loop to stop
                        break
                    posts.append(
                        {
                            "id": post["id"],
                            "created_at": post["created_at"],
                            "content": post.get("content", ""),
                            "url": post.get("url", ""),
                            "favourites": post.get("favourites_count", 0),
                            "boosts": post.get("reblogs_count", 0),
                            "replies": post.get("replies_count", 0),
                        }
                    )

                if not batch:
                    break
                params["max_id"] = batch[-1]["id"]

        logger.info("Mastodon: collected %d posts", len(posts))
        return {
            "platform": "mastodon",
            "handle": f"@{handle}@{instance}",
            "collected_at": _iso(_utcnow()),
            "period_weeks": 2,
            "posts": posts,
            "account": {
                "followers": account.get("followers_count", 0),
                "following": account.get("following_count", 0),
                "statuses_count": account.get("statuses_count", 0),
            },
        }

    except Exception as exc:
        logger.error("Mastodon collection failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Bluesky
# ---------------------------------------------------------------------------

def collect_bluesky(handle: str) -> dict[str, Any] | None:
    """
    Collect the last 2 weeks of posts for a Bluesky account.
    Uses the public AppView API — no authentication required.
    """
    try:
        base = "https://public.api.bsky.app/xrpc"
        cutoff = _cutoff(weeks=2)
        posts: list[dict] = []

        with httpx.Client(timeout=30) as client:
            # Resolve handle → DID
            r = client.get(
                f"{base}/com.atproto.identity.resolveHandle",
                params={"handle": handle},
            )
            r.raise_for_status()
            did = r.json()["did"]

            # Fetch author feed
            cursor: str | None = None
            while True:
                params: dict[str, Any] = {"actor": did, "limit": 50}
                if cursor:
                    params["cursor"] = cursor

                r = client.get(f"{base}/app.bsky.feed.getAuthorFeed", params=params)
                r.raise_for_status()
                data = r.json()
                feed = data.get("feed", [])

                if not feed:
                    break

                stop = False
                for item in feed:
                    post = item.get("post", {})
                    record = post.get("record", {})
                    created_str = record.get("createdAt", "")
                    if not created_str:
                        continue
                    try:
                        created = datetime.fromisoformat(
                            created_str.replace("Z", "+00:00")
                        )
                    except ValueError:
                        continue

                    if created < cutoff:
                        stop = True
                        break

                    # Skip reposts of others' content
                    if item.get("reason", {}).get("$type") == "app.bsky.feed.defs#reasonRepost":
                        continue

                    counts = post.get("likeCount", 0), post.get("repostCount", 0), post.get("replyCount", 0)
                    posts.append(
                        {
                            "uri": post.get("uri", ""),
                            "created_at": created_str,
                            "text": record.get("text", ""),
                            "likes": post.get("likeCount", 0),
                            "reposts": post.get("repostCount", 0),
                            "replies": post.get("replyCount", 0),
                        }
                    )

                if stop or not data.get("cursor"):
                    break
                cursor = data["cursor"]

        logger.info("Bluesky: collected %d posts", len(posts))
        return {
            "platform": "bluesky",
            "handle": handle,
            "collected_at": _iso(_utcnow()),
            "period_weeks": 2,
            "posts": posts,
        }

    except Exception as exc:
        logger.error("Bluesky collection failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Buttondown
# ---------------------------------------------------------------------------

def collect_buttondown(api_key: str) -> dict[str, Any] | None:
    """
    Collect the last 4 newsletters from Buttondown with open/click analytics.
    """
    try:
        headers = {"Authorization": f"Token {api_key}"}
        newsletters: list[dict] = []

        with httpx.Client(timeout=30, headers=headers) as client:
            # Fetch recent emails (sorted newest-first by default)
            r = client.get(
                "https://api.buttondown.email/v1/emails",
                params={"status": "sent", "page_size": 4},
            )
            r.raise_for_status()
            emails = r.json().get("results", [])

            for email in emails:
                email_id = email["id"]
                # Fetch analytics for each email
                stats: dict = {}
                try:
                    ar = client.get(
                        f"https://api.buttondown.email/v1/emails/{email_id}/analytics"
                    )
                    ar.raise_for_status()
                    stats = ar.json()
                except Exception as ae:
                    logger.warning(
                        "Could not fetch analytics for email %s: %s", email_id, ae
                    )

                newsletters.append(
                    {
                        "id": email_id,
                        "subject": email.get("subject", ""),
                        "send_date": email.get("publish_date") or email.get("creation_date", ""),
                        "status": email.get("status", ""),
                        "open_rate": stats.get("open_rate"),
                        "click_rate": stats.get("click_rate"),
                        "recipients": stats.get("recipients"),
                    }
                )

            # Current subscriber count
            sr = client.get("https://api.buttondown.email/v1/subscribers", params={"page_size": 1})
            sr.raise_for_status()
            subscriber_count = sr.json().get("count", None)

        logger.info("Buttondown: collected %d newsletters", len(newsletters))
        return {
            "platform": "buttondown",
            "collected_at": _iso(_utcnow()),
            "subscriber_count": subscriber_count,
            "newsletters": newsletters,
        }

    except Exception as exc:
        logger.error("Buttondown collection failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Jetpack / WordPress.com Stats
# ---------------------------------------------------------------------------

def collect_jetpack(site: str, access_token: str) -> dict[str, Any] | None:
    """
    Collect last 2 weeks of daily page views and top posts from Jetpack Stats.
    """
    try:
        headers = {"Authorization": f"Bearer {access_token}"}
        base = f"https://public-api.wordpress.com/rest/v1.1/sites/{site}/stats"

        today = _utcnow().date()
        start = today - timedelta(weeks=2)

        with httpx.Client(timeout=30, headers=headers) as client:
            # Daily views
            r = client.get(
                f"{base}/visits",
                params={
                    "unit": "day",
                    "quantity": 14,
                    "date": today.isoformat(),
                },
            )
            r.raise_for_status()
            visits_data = r.json()

            # Top posts/pages
            tp = client.get(
                f"{base}/top-posts",
                params={
                    "period": "week",
                    "date": today.isoformat(),
                    "num": 10,
                    "max": 10,
                },
            )
            tp.raise_for_status()
            top_posts_data = tp.json()

        daily_views = []
        for row in visits_data.get("data", []):
            if isinstance(row, list) and len(row) >= 2:
                daily_views.append({"date": row[0], "views": row[1]})

        top_posts = []
        for post in top_posts_data.get("top-posts", []):
            top_posts.append(
                {
                    "title": post.get("title", ""),
                    "href": post.get("href", ""),
                    "views": post.get("views", 0),
                }
            )

        total_views = sum(d["views"] for d in daily_views)
        logger.info(
            "Jetpack: collected %d days of data, %d total views",
            len(daily_views),
            total_views,
        )
        return {
            "platform": "jetpack",
            "site": site,
            "collected_at": _iso(_utcnow()),
            "period_weeks": 2,
            "total_views": total_views,
            "daily_views": daily_views,
            "top_posts": top_posts,
        }

    except Exception as exc:
        logger.error("Jetpack collection failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# LinkedIn CSV
# ---------------------------------------------------------------------------

# LinkedIn's export columns vary slightly by export type; we normalise them.
_LINKEDIN_COLUMN_MAP = {
    # Post content/title
    "post title": "title",
    "content": "title",
    "post content": "title",
    # Date
    "date": "date",
    "published date": "date",
    # Metrics
    "impressions": "impressions",
    "clicks": "clicks",
    "reactions": "reactions",
    "comments": "comments",
    "shares": "shares",
    "reposts": "shares",
    "ctr (clicks / impressions)": "ctr",
    "engagement rate": "engagement_rate",
}


def collect_linkedin(linkedin_drops_dir: str | Path = "linkedin_drops") -> dict[str, Any] | None:
    """
    Read the most recently modified LinkedIn post analytics CSV export
    from the linkedin_drops/ directory.
    """
    drops_path = Path(linkedin_drops_dir)
    csv_files = sorted(
        drops_path.glob("*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not csv_files:
        logger.info("LinkedIn: no CSV files found in %s — skipping", drops_path)
        return None

    csv_path = csv_files[0]
    logger.info("LinkedIn: reading %s", csv_path)

    try:
        # LinkedIn CSVs sometimes have a metadata header; skip non-data rows
        df = pd.read_csv(csv_path, skiprows=0)

        # Normalise column names
        df.columns = [c.strip().lower() for c in df.columns]
        rename = {k: v for k, v in _LINKEDIN_COLUMN_MAP.items() if k in df.columns}
        df = df.rename(columns=rename)

        # Keep only mapped columns that exist
        keep = [c for c in _LINKEDIN_COLUMN_MAP.values() if c in df.columns]
        df = df[keep].copy()

        # Drop rows where all metric columns are NaN (often trailing summary rows)
        metric_cols = [c for c in ["impressions", "clicks", "reactions", "comments", "shares"] if c in df.columns]
        df = df.dropna(subset=metric_cols, how="all")

        # Coerce numeric columns
        for col in metric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        posts = df.to_dict(orient="records")

        summary = {
            "total_impressions": int(df["impressions"].sum()) if "impressions" in df.columns else None,
            "total_clicks": int(df["clicks"].sum()) if "clicks" in df.columns else None,
            "total_reactions": int(df["reactions"].sum()) if "reactions" in df.columns else None,
            "total_comments": int(df["comments"].sum()) if "comments" in df.columns else None,
            "total_shares": int(df["shares"].sum()) if "shares" in df.columns else None,
        }

        logger.info("LinkedIn: parsed %d posts from %s", len(posts), csv_path.name)
        return {
            "platform": "linkedin",
            "source_file": csv_path.name,
            "collected_at": _iso(_utcnow()),
            "summary": summary,
            "posts": posts,
        }

    except Exception as exc:
        logger.error("LinkedIn CSV parsing failed (%s): %s", csv_path, exc)
        return None


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

PLATFORM_COLLECTORS = {
    "mastodon": "collect_mastodon",
    "bluesky": "collect_bluesky",
    "buttondown": "collect_buttondown",
    "jetpack": "collect_jetpack",
    "linkedin": "collect_linkedin",
}


def collect_all(config: dict, platform: str | None = None) -> dict[str, Any]:
    """
    Run all (or a single) collector and return a dict keyed by platform name.
    Failures are logged but do not raise; the platform key is omitted from
    the result if collection fails.
    """
    results: dict[str, Any] = {}

    def _run(name: str) -> None:
        if name == "mastodon":
            data = collect_mastodon(
                config.get("mastodon_instance", ""),
                config.get("mastodon_handle", ""),
            )
        elif name == "bluesky":
            data = collect_bluesky(config.get("bluesky_handle", ""))
        elif name == "buttondown":
            data = collect_buttondown(config.get("buttondown_api_key", ""))
        elif name == "jetpack":
            data = collect_jetpack(
                config.get("jetpack_site", ""),
                config.get("jetpack_access_token", ""),
            )
        elif name == "linkedin":
            data = collect_linkedin()
        else:
            logger.error("Unknown platform: %s", name)
            return

        if data is not None:
            results[name] = data

    if platform:
        _run(platform)
    else:
        for name in PLATFORM_COLLECTORS:
            _run(name)

    return results
