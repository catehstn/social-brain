from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since, _strip_html_simple

logger = logging.getLogger(__name__)


def collect_mentions(
    domains: list[str],
    since: datetime | None = None,
    mastodon_instance: str = "",
    mastodon_access_token: str = "",
    bluesky_handle: str = "",
    bluesky_app_password: str = "",
    gsc_credentials_file: str = "",
) -> dict[str, Any] | None:
    """
    Collect mentions of `domains` across:
      - Hacker News (via Algolia, no auth)
      - Mastodon notifications (requires mastodon_access_token)
      - Bluesky notifications (requires bluesky_app_password)
      - Google Search Console (requires gsc_credentials_file)

    `domains` is a list like ["cate.blog", "driyourcareer.com"].
    """
    if not domains:
        logger.info("Mentions: no monitored_domains configured — skipping")
        return None

    if since is None:
        since = _default_since()

    sources: dict[str, Any] = {}

    # --- Hacker News (Algolia search, no auth) ---
    try:
        hn_hits: list[dict] = []
        since_ts = int(since.timestamp())
        with httpx.Client(timeout=30) as client:
            for domain in domains:
                for tag in ("story", "comment"):
                    r = client.get(
                        "https://hn.algolia.com/api/v1/search_by_date",
                        params={
                            "query": domain,
                            "tags": tag,
                            "numericFilters": f"created_at_i>{since_ts}",
                            "hitsPerPage": 50,
                            "restrictSearchableAttributes": "url,title,comment_text",
                        },
                    )
                    r.raise_for_status()
                    for hit in r.json().get("hits", []):
                        # Only keep hits where the domain actually appears in the URL or text
                        url = hit.get("url", "")
                        text = hit.get("title") or hit.get("comment_text") or ""
                        if domain not in url and domain not in text:
                            continue
                        hn_hits.append({
                            "type": tag,
                            "domain": domain,
                            "title": hit.get("title") or text[:120],
                            "url": hit.get("url") or f"https://news.ycombinator.com/item?id={hit.get('objectID')}",
                            "hn_url": f"https://news.ycombinator.com/item?id={hit.get('objectID')}",
                            "points": hit.get("points", 0),
                            "num_comments": hit.get("num_comments", 0),
                            "created_at": hit.get("created_at", "")[:10],
                            "author": hit.get("author", ""),
                        })
        # Deduplicate by HN item ID
        seen: set[str] = set()
        deduped = []
        for h in hn_hits:
            key = h["hn_url"]
            if key not in seen:
                seen.add(key)
                deduped.append(h)
        deduped.sort(key=lambda h: h.get("points") or 0, reverse=True)
        sources["hacker_news"] = deduped
        logger.info("Mentions: %d HN hits for %s", len(deduped), domains)
    except Exception as exc:
        logger.error("Mentions/HN failed: %s", exc)

    # --- Mastodon notifications (requires access token) ---
    if mastodon_instance and mastodon_access_token:
        try:
            notifications = []
            url = f"https://{mastodon_instance}/api/v1/notifications"
            params = {"types[]": "mention", "limit": 40}
            max_pages = 5  # cap at 200 notifications to avoid rate limiting
            pages_fetched = 0
            with httpx.Client(
                timeout=30,
                headers={"Authorization": f"Bearer {mastodon_access_token}"},
            ) as client:
                while url and pages_fetched < max_pages:
                    r = client.get(url, params=params)
                    r.raise_for_status()
                    batch = r.json()
                    pages_fetched += 1
                    for n in batch:
                        created = n.get("created_at", "")
                        if created and created < _iso(since):
                            url = None
                            break
                        status = n.get("status", {})
                        notifications.append({
                            "created_at": created[:10],
                            "from": n.get("account", {}).get("acct", ""),
                            "content": _strip_html_simple(status.get("content", ""))[:300],
                            "url": status.get("url", ""),
                        })
                    else:
                        # Follow Link header pagination
                        link = r.headers.get("Link", "")
                        next_url = re.search(r'<([^>]+)>;\s*rel="next"', link)
                        url = next_url.group(1) if next_url else None
                    params = {}  # params already in URL after first request
            sources["mastodon"] = notifications
            logger.info("Mentions: %d Mastodon mentions", len(notifications))
        except Exception as exc:
            logger.error("Mentions/Mastodon failed: %s", exc)

    # --- Bluesky notifications (requires app password) ---
    if bluesky_handle and bluesky_app_password:
        try:
            # Create session
            sess = httpx.post(
                "https://bsky.social/xrpc/com.atproto.server.createSession",
                json={"identifier": bluesky_handle, "password": bluesky_app_password},
                timeout=15,
            )
            sess.raise_for_status()
            access_jwt = sess.json()["accessJwt"]

            notifications = []
            cursor = None
            while True:
                params: dict = {"limit": 50}
                if cursor:
                    params["cursor"] = cursor
                r = httpx.get(
                    "https://bsky.social/xrpc/app.bsky.notification.listNotifications",
                    headers={"Authorization": f"Bearer {access_jwt}"},
                    params=params,
                    timeout=30,
                )
                r.raise_for_status()
                data = r.json()
                batch = data.get("notifications", [])
                stop = False
                for n in batch:
                    if n.get("reason") != "mention":
                        continue
                    indexed = n.get("indexedAt", "")
                    if indexed and indexed < _iso(since):
                        stop = True
                        break
                    record = n.get("record", {})
                    notifications.append({
                        "created_at": indexed[:10],
                        "from": n.get("author", {}).get("handle", ""),
                        "content": record.get("text", "")[:300],
                        "url": f"https://bsky.app/profile/{n.get('author',{}).get('handle','')}/post/{n.get('uri','').split('/')[-1]}",
                    })
                cursor = data.get("cursor")
                if stop or not cursor:
                    break
            sources["bluesky"] = notifications
            logger.info("Mentions: %d Bluesky mentions", len(notifications))
        except Exception as exc:
            logger.error("Mentions/Bluesky failed: %s", exc)

    # --- Google Search Console (requires credentials JSON file) ---
    if gsc_credentials_file and domains:
        try:
            from google.oauth2.service_account import Credentials
            from googleapiclient.discovery import build as gsc_build
            gsc_credentials_file = os.path.expanduser(gsc_credentials_file)

            creds = Credentials.from_service_account_file(
                gsc_credentials_file,
                scopes=["https://www.googleapis.com/auth/webmasters.readonly"],
            )
            service = gsc_build("searchconsole", "v1", credentials=creds)
            gsc_results = []
            for domain in domains:
                # Try domain property first, then URL-prefix variants
                candidates = [
                    f"sc-domain:{domain}",
                    f"https://{domain}/",
                    f"http://{domain}/",
                ]
                body = {
                    "startDate": since.strftime("%Y-%m-%d"),
                    "endDate": _utcnow().strftime("%Y-%m-%d"),
                    "dimensions": ["query", "page"],
                    "rowLimit": 25,
                }
                fetched = False
                for site_url in candidates:
                    try:
                        resp = (
                            service.searchanalytics()
                            .query(siteUrl=site_url, body=body)
                            .execute()
                        )
                        logger.info("GSC: using property %s for %s", site_url, domain)
                        for row in resp.get("rows", []):
                            keys = row.get("keys", [])
                            gsc_results.append({
                                "domain": domain,
                                "query": keys[0] if keys else "",
                                "page": keys[1] if len(keys) > 1 else "",
                                "clicks": row.get("clicks", 0),
                                "impressions": row.get("impressions", 0),
                                "ctr": round(row.get("ctr", 0) * 100, 1),
                                "position": round(row.get("position", 0), 1),
                            })
                        fetched = True
                        break
                    except Exception:
                        continue
                if not fetched:
                    logger.warning("GSC query failed for %s: no accessible property found (tried domain, https, http)", domain)
            sources["google_search_console"] = gsc_results
            logger.info("Mentions: %d GSC rows across %d domains", len(gsc_results), len(domains))
        except ImportError:
            logger.error("Mentions/GSC: google-api-python-client not installed. Run: pip install google-api-python-client google-auth")
        except Exception as exc:
            logger.error("Mentions/GSC failed: %s", exc)

    if not sources:
        return None

    return {
        "platform": "mentions",
        "domains": domains,
        "collected_at": _iso(_utcnow()),
        "since": _iso(since),
        "sources": sources,
    }
