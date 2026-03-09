from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)


def collect_mastodon(
    instance: str,
    handle: str,
    since: datetime | None = None,
    access_token: str = "",
) -> dict[str, Any] | None:
    """
    Collect public posts for a Mastodon account back to `since`
    (default: 2 weeks ago). Uses the public API — no authentication required.
    If access_token is provided, also collects new follows during the period.
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
                    attachments = post.get("media_attachments", [])
                    posts.append(
                        {
                            "id": post["id"],
                            "created_at": post["created_at"],
                            "content": post.get("content", ""),
                            "url": post.get("url", ""),
                            "favourites": post.get("favourites_count", 0),
                            "boosts": post.get("reblogs_count", 0),
                            "replies": post.get("replies_count", 0),
                            "has_attachment": bool(attachments),
                            "attachment_types": list({a.get("type") for a in attachments if a.get("type")}),
                        }
                    )

                if not batch:
                    break
                params["max_id"] = batch[-1]["id"]

        logger.info("Mastodon: collected %d posts since %s", len(posts), _iso(since))

        # New follows during the period (requires access token)
        new_follows: list[dict] = []
        if access_token:
            auth_headers = {"Authorization": f"Bearer {access_token}"}
            with httpx.Client(timeout=30) as client:
                params_f: dict[str, Any] = {"types[]": "follow", "limit": 80}
                while True:
                    r = client.get(
                        f"{base}/api/v1/notifications",
                        params=params_f,
                        headers=auth_headers,
                    )
                    r.raise_for_status()
                    batch = r.json()
                    if not batch:
                        break
                    stop = False
                    for n in batch:
                        created = datetime.fromisoformat(
                            n["created_at"].replace("Z", "+00:00")
                        )
                        if created < since:
                            stop = True
                            break
                        acct = n.get("account", {})
                        new_follows.append({
                            "followed_at": n["created_at"],
                            "account": acct.get("acct", ""),
                            "display_name": acct.get("display_name", ""),
                            "followers": acct.get("followers_count", 0),
                        })
                    if stop:
                        break
                    link = r.headers.get("Link", "")
                    m = re.search(r'<[^>]+\?[^>]*max_id=(\d+)[^>]*>;\s*rel="next"', link)
                    if not m:
                        break
                    params_f["max_id"] = m.group(1)
            logger.info("Mastodon: collected %d new follows since %s", len(new_follows), _iso(since))

        result: dict[str, Any] = {
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
        if new_follows:
            result["new_follows"] = new_follows
            result["new_follows_count"] = len(new_follows)
        return result

    except Exception as exc:
        logger.error("Mastodon collection failed: %s", exc)
        return None
