from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)


def collect_bluesky(
    handle: str,
    since: datetime | None = None,
    app_password: str = "",
) -> dict[str, Any] | None:
    """
    Collect posts for a Bluesky account back to `since`
    (default: 2 weeks ago). Uses the public AppView API — no auth required.
    If app_password is provided, also collects new follows during the period.
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

                    embed = post.get("embed", {})
                    embed_type = embed.get("$type", "")
                    if "images" in embed_type:
                        attachment_types = ["image"]
                    elif "video" in embed_type:
                        attachment_types = ["video"]
                    elif "external" in embed_type:
                        attachment_types = ["link"]
                    elif "recordWithMedia" in embed_type:
                        attachment_types = ["quote+media"]
                    elif "record" in embed_type:
                        attachment_types = ["quote"]
                    else:
                        attachment_types = []

                    posts.append(
                        {
                            "uri": post.get("uri", ""),
                            "created_at": created_str,
                            "text": record.get("text", ""),
                            "likes": post.get("likeCount", 0),
                            "reposts": post.get("repostCount", 0),
                            "replies": post.get("replyCount", 0),
                            "has_attachment": bool(embed_type),
                            "attachment_types": attachment_types,
                        }
                    )

                if stop or not data.get("cursor"):
                    break
                cursor = data["cursor"]

        logger.info("Bluesky: collected %d posts since %s", len(posts), _iso(since))

        # New follows during the period (requires app password)
        new_follows: list[dict] = []
        if app_password:
            auth_base = "https://bsky.social/xrpc"
            with httpx.Client(timeout=30) as client:
                r = client.post(
                    f"{auth_base}/com.atproto.server.createSession",
                    json={"identifier": handle, "password": app_password},
                )
                r.raise_for_status()
                token = r.json()["accessJwt"]
                auth_headers = {"Authorization": f"Bearer {token}"}

                cursor_f: str | None = None
                while True:
                    params_f: dict[str, Any] = {"limit": 100, "reasons": "follow"}
                    if cursor_f:
                        params_f["cursor"] = cursor_f
                    r = client.get(
                        f"{auth_base}/app.bsky.notification.listNotifications",
                        params=params_f,
                        headers=auth_headers,
                    )
                    r.raise_for_status()
                    data_f = r.json()
                    notifs = data_f.get("notifications", [])
                    if not notifs:
                        break
                    stop = False
                    for n in notifs:
                        if n.get("reason") != "follow":
                            continue
                        indexed = n.get("indexedAt", "")
                        try:
                            ts = datetime.fromisoformat(indexed.replace("Z", "+00:00"))
                        except Exception:
                            continue
                        if ts < since:
                            stop = True
                            break
                        author = n.get("author", {})
                        new_follows.append({
                            "followed_at": indexed,
                            "handle": author.get("handle", ""),
                            "display_name": author.get("displayName", ""),
                            "followers": author.get("followersCount", 0),
                        })
                    if stop or not data_f.get("cursor"):
                        break
                    cursor_f = data_f["cursor"]
            logger.info("Bluesky: collected %d new follows since %s", len(new_follows), _iso(since))

        result: dict[str, Any] = {
            "platform": "bluesky",
            "handle": handle,
            "collected_at": _iso(_utcnow()),
            "since": _iso(since),
            "posts": posts,
        }
        if new_follows:
            result["new_follows"] = new_follows
            result["new_follows_count"] = len(new_follows)
        return result

    except Exception as exc:
        logger.error("Bluesky collection failed: %s", exc)
        return None
