from __future__ import annotations

import getpass
import logging
import pathlib
import re
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)

_CONFIG_PATH = pathlib.Path(__file__).parent.parent / "config.yaml"


def _reauth_jetpack(client_id: str, client_secret: str, username: str, password: str) -> str | None:
    """Exchange WordPress.com credentials for a new access token. Returns token or None."""
    try:
        r = httpx.post(
            "https://public-api.wordpress.com/oauth2/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "password",
                "username": username,
                "password": password,
            },
            timeout=30,
        )
        if not r.is_success:
            logger.error("Jetpack re-auth failed: HTTP %s — %s", r.status_code, r.text[:300])
            return None
        token = r.json().get("access_token")
        if token:
            logger.info("Jetpack: re-auth succeeded, saving new token to config.yaml")
            _save_token_to_config(token)
        return token
    except Exception as exc:
        logger.error("Jetpack re-auth failed: %s", exc)
        return None


def _save_token_to_config(new_token: str) -> None:
    """Replace jetpack_access_token value in config.yaml in-place."""
    try:
        text = _CONFIG_PATH.read_text()
        updated = re.sub(
            r"(?m)^(\s*jetpack_access_token\s*:\s*).*$",
            rf"\g<1>{new_token}",
            text,
        )
        _CONFIG_PATH.write_text(updated)
    except Exception as exc:
        logger.warning("Jetpack: could not update config.yaml with new token: %s", exc)


def collect_jetpack(
    site: str,
    access_token: str,
    since: datetime | None = None,
    client_id: str = "",
    client_secret: str = "",
    username: str = "",
) -> dict[str, Any] | None:
    """
    Collect daily page views and top posts from Jetpack Stats back to `since`
    (default: 2 weeks ago).

    If client_id, client_secret, and username are configured, a 403 response
    will prompt for the WordPress.com password at the terminal, re-auth, save
    the new token to config.yaml, and retry automatically.
    """
    if since is None:
        since = _default_since()

    can_reauth = all([client_id, client_secret, username])

    def _fetch(token: str) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {token}"}
        base = f"https://public-api.wordpress.com/rest/v1.1/sites/{site}/stats"
        today = _utcnow().date()
        quantity = (today - since.date()).days + 1

        with httpx.Client(timeout=30, headers=headers) as client:
            r = client.get(
                f"{base}/visits",
                params={"unit": "day", "quantity": quantity, "date": today.isoformat()},
            )
            r.raise_for_status()
            visits_data = r.json()

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
                params={"period": "day", "num": quantity, "date": today.isoformat()},
            )
            rr.raise_for_status()
            referrers_data = rr.json()

            fe = client.get(f"{base}/followers", params={"type": "email", "max": 0})
            fe.raise_for_status()
            email_followers_data = fe.json()

            fw = client.get(f"{base}/followers", params={"type": "wpcom", "max": 0})
            fw.raise_for_status()
            wpcom_followers_data = fw.json()

        daily_views = []
        for row in visits_data.get("data", []):
            if isinstance(row, list) and len(row) >= 2:
                daily_views.append({"date": row[0], "views": row[1]})

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

        top_posts = sorted(top_posts_raw.values(), key=lambda p: p["views"], reverse=True)[:10]

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

        email_subscribers = email_followers_data.get("total", 0)
        wpcom_followers = wpcom_followers_data.get("total", 0)
        total_views = sum(d["views"] for d in daily_views)

        logger.info(
            "Jetpack: collected %d days of data (%d total views, %d referrers, %d email subs, %d wpcom followers)",
            len(daily_views), total_views, len(referrers), email_subscribers, wpcom_followers,
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
            "email_subscribers": email_subscribers,
            "wpcom_followers": wpcom_followers,
        }

    try:
        return _fetch(access_token)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 403 and can_reauth:
            logger.warning("Jetpack: 403 — token expired, prompting for WordPress.com password")
            password = getpass.getpass(f"WordPress.com password for {username}: ")
            new_token = _reauth_jetpack(client_id, client_secret, username, password)
            if new_token:
                try:
                    result = _fetch(new_token)
                    if result is not None:
                        result["new_access_token"] = new_token
                    return result
                except Exception as retry_exc:
                    logger.error("Jetpack collection failed after re-auth: %s", retry_exc)
                    return None
        logger.error("Jetpack collection failed: %s", exc)
        return None
    except Exception as exc:
        logger.error("Jetpack collection failed: %s", exc)
        return None
