"""
collect.py — data collectors for each social/content platform.

Each collector returns a dict of structured data on success,
or None on failure (errors are logged but not re-raised so
a single platform outage never kills the whole run).
"""

from __future__ import annotations

import logging
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


def _default_since() -> datetime:
    """Default lookback: 2 weeks."""
    return _utcnow() - timedelta(weeks=2)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Mastodon
# ---------------------------------------------------------------------------

def collect_mastodon(
    instance: str,
    handle: str,
    since: datetime | None = None,
) -> dict[str, Any] | None:
    """
    Collect public posts for a Mastodon account back to `since`
    (default: 2 weeks ago). Uses the public API — no authentication required.
    """
    if since is None:
        since = _default_since()

    try:
        base = f"https://{instance}"
        with httpx.Client(timeout=30) as client:
            r = client.get(
                f"{base}/api/v1/accounts/lookup",
                params={"acct": handle},
            )
            r.raise_for_status()
            account = r.json()
            account_id = account["id"]

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
                    if created < since:
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

        logger.info("Mastodon: collected %d posts since %s", len(posts), _iso(since))
        return {
            "platform": "mastodon",
            "handle": f"@{handle}@{instance}",
            "collected_at": _iso(_utcnow()),
            "since": _iso(since),
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

def collect_bluesky(
    handle: str,
    since: datetime | None = None,
) -> dict[str, Any] | None:
    """
    Collect posts for a Bluesky account back to `since`
    (default: 2 weeks ago). Uses the public AppView API — no auth required.
    """
    if since is None:
        since = _default_since()

    try:
        base = "https://public.api.bsky.app/xrpc"
        posts: list[dict] = []

        with httpx.Client(timeout=30) as client:
            r = client.get(
                f"{base}/com.atproto.identity.resolveHandle",
                params={"handle": handle},
            )
            r.raise_for_status()
            did = r.json()["did"]

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

                    if created < since:
                        stop = True
                        break

                    # Skip reposts of others' content
                    if item.get("reason", {}).get("$type") == "app.bsky.feed.defs#reasonRepost":
                        continue

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

        logger.info("Bluesky: collected %d posts since %s", len(posts), _iso(since))
        return {
            "platform": "bluesky",
            "handle": handle,
            "collected_at": _iso(_utcnow()),
            "since": _iso(since),
            "posts": posts,
        }

    except Exception as exc:
        logger.error("Bluesky collection failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Buttondown
# ---------------------------------------------------------------------------

def _collect_buttondown_newsletter(
    client: httpx.Client,
    newsletter_name: str,
    newsletter_key: str,
    since: datetime | None,
    use_since: bool,
) -> tuple[list[dict], int | None]:
    """Collect emails and subscriber count for a single Buttondown newsletter."""
    headers = {"Authorization": f"Token {newsletter_key}"}
    newsletters: list[dict] = []
    page = 1

    while True:
        r = client.get(
            "https://api.buttondown.email/v1/emails",
            params={"status": "sent", "page_size": 20, "page": page},
            headers=headers,
        )
        r.raise_for_status()
        data = r.json()
        emails = data.get("results", [])

        if not emails:
            break

        stop = False
        for email in emails:
            send_date_str = email.get("publish_date") or email.get("creation_date", "")
            if use_since and send_date_str:
                try:
                    send_date = datetime.fromisoformat(
                        send_date_str.replace("Z", "+00:00")
                    )
                    if send_date < since:
                        stop = True
                        break
                except ValueError:
                    pass

            stats = email.get("analytics", {})
            recipients = stats.get("recipients") or 0
            opens = stats.get("opens") or 0
            clicks = stats.get("clicks") or 0

            newsletters.append(
                {
                    "newsletter": newsletter_name,
                    "id": email["id"],
                    "subject": email.get("subject", ""),
                    "send_date": send_date_str,
                    "url": email.get("absolute_url", ""),
                    "recipients": recipients,
                    "opens": opens,
                    "clicks": clicks,
                    "open_rate": round(opens / recipients, 4) if recipients else None,
                    "click_rate": round(clicks / recipients, 4) if recipients else None,
                    "unsubscribes": stats.get("unsubscriptions") or 0,
                    "new_subscribers": stats.get("subscriptions") or 0,
                }
            )

            if not use_since and len(newsletters) >= 4:
                stop = True
                break

        if stop or not data.get("next"):
            break
        page += 1

    sr = client.get(
        "https://api.buttondown.email/v1/subscribers",
        params={"page_size": 1},
        headers=headers,
    )
    sr.raise_for_status()
    subscriber_count = sr.json().get("count", None)

    return newsletters, subscriber_count


def collect_buttondown(
    api_key: str,
    since: datetime | None = None,
) -> dict[str, Any] | None:
    """
    Collect sent newsletters from all Buttondown newsletters on the account.
    Uses the /newsletters endpoint to discover all newsletters and their keys,
    then collects from each. If `since` is provided, fetches all emails back
    to that date; otherwise fetches the last 4 per newsletter.
    """
    use_since = since is not None

    try:
        all_newsletters: list[dict] = []
        subscriber_counts: dict[str, int] = {}

        with httpx.Client(timeout=30) as client:
            # Discover all newsletters on the account
            r = client.get(
                "https://api.buttondown.email/v1/newsletters",
                headers={"Authorization": f"Token {api_key}"},
            )
            r.raise_for_status()
            account_newsletters = r.json().get("results", [])

            for nl in account_newsletters:
                nl_name = nl.get("name", nl.get("domain", nl["id"]))
                nl_key = nl.get("api_key", api_key)
                try:
                    emails, count = _collect_buttondown_newsletter(
                        client, nl_name, nl_key, since, use_since
                    )
                    all_newsletters.extend(emails)
                    if count is not None:
                        subscriber_counts[nl_name] = count
                    logger.info(
                        "Buttondown [%s]: %d newsletters, %s subscribers",
                        nl_name, len(emails), count,
                    )
                except Exception as exc:
                    logger.warning("Buttondown [%s] failed: %s", nl_name, exc)

        # Sort combined list newest-first
        all_newsletters.sort(key=lambda e: e.get("send_date", ""), reverse=True)

        result: dict[str, Any] = {
            "platform": "buttondown",
            "collected_at": _iso(_utcnow()),
            "subscriber_counts": subscriber_counts,
            "newsletters": all_newsletters,
        }
        if use_since:
            result["since"] = _iso(since)
        return result

    except Exception as exc:
        logger.error("Buttondown collection failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Jetpack / WordPress.com Stats
# ---------------------------------------------------------------------------

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
            "Jetpack: collected %d days of data (%d total views)",
            len(daily_views),
            total_views,
        )
        return {
            "platform": "jetpack",
            "site": site,
            "collected_at": _iso(_utcnow()),
            "since": _iso(since),
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

_LINKEDIN_COLUMN_MAP = {
    "post title": "title",
    "content": "title",
    "post content": "title",
    "date": "date",
    "published date": "date",
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
        df = pd.read_csv(csv_path, skiprows=0)

        df.columns = [c.strip().lower() for c in df.columns]
        rename = {k: v for k, v in _LINKEDIN_COLUMN_MAP.items() if k in df.columns}
        df = df.rename(columns=rename)

        keep = [c for c in _LINKEDIN_COLUMN_MAP.values() if c in df.columns]
        df = df[keep].copy()

        metric_cols = [c for c in ["impressions", "clicks", "reactions", "comments", "shares"] if c in df.columns]
        df = df.dropna(subset=metric_cols, how="all")

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


def collect_all(
    config: dict,
    platform: str | None = None,
    since: datetime | None = None,
) -> dict[str, Any]:
    """
    Run all (or a single) collector and return a dict keyed by platform name.
    `since` overrides the default lookback window for all date-based collectors.
    Failures are logged but do not raise.
    """
    results: dict[str, Any] = {}

    def _run(name: str) -> None:
        if name == "mastodon":
            data = collect_mastodon(
                config.get("mastodon_instance", ""),
                config.get("mastodon_handle", ""),
                since=since,
            )
        elif name == "bluesky":
            data = collect_bluesky(config.get("bluesky_handle", ""), since=since)
        elif name == "buttondown":
            data = collect_buttondown(config.get("buttondown_api_key", ""), since=since)
        elif name == "jetpack":
            data = collect_jetpack(
                config.get("jetpack_site", ""),
                config.get("jetpack_access_token", ""),
                since=since,
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
