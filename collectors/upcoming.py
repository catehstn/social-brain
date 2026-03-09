from __future__ import annotations

import logging
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _strip_html

logger = logging.getLogger(__name__)


def collect_upcoming(
    jetpack_site: str = "",
    jetpack_access_token: str = "",
    buttondown_api_key: str = "",
    buffer_token: str = "",
) -> dict[str, Any] | None:
    """
    Collect scheduled/upcoming content from:
      - WordPress (scheduled posts via Jetpack API)
      - Buttondown (scheduled emails)
      - Buffer (queued social posts, if token configured)
    """
    sources: dict[str, Any] = {}

    # --- WordPress scheduled posts ---
    if jetpack_site and jetpack_access_token:
        try:
            r = httpx.get(
                f"https://public-api.wordpress.com/rest/v1.1/sites/{jetpack_site}/posts",
                params={
                    "status": "future",  # WordPress uses 'future' for scheduled future posts
                    "fields": "ID,title,URL,date,content",
                    "number": 20,
                    "order_by": "date",
                    "order": "ASC",
                },
                headers={"Authorization": f"Bearer {jetpack_access_token}"},
                timeout=30,
            )
            r.raise_for_status()
            raw_posts = r.json().get("posts", [])
            posts = [
                {
                    "title": p.get("title", ""),
                    "url": p.get("URL", ""),
                    "scheduled_date": p.get("date", ""),
                    "content": _strip_html(p.get("content", "")),
                }
                for p in raw_posts
            ]
            sources["wordpress"] = posts
            logger.info("Upcoming: %d scheduled WordPress posts", len(posts))
        except Exception as exc:
            logger.error("Upcoming/WordPress failed: %s", exc)

    # --- Buttondown scheduled emails ---
    if buttondown_api_key:
        try:
            r = httpx.get(
                "https://api.buttondown.email/v1/emails",
                params={"status": "scheduled"},
                headers={"Authorization": f"Token {buttondown_api_key}"},
                timeout=30,
            )
            r.raise_for_status()
            raw_emails = r.json().get("results", [])
            emails = [
                {
                    "subject": e.get("subject", ""),
                    "scheduled_date": e.get("publish_date") or e.get("creation_date", ""),
                    "content": _strip_html(e.get("body", "")),
                }
                for e in raw_emails
            ]
            sources["buttondown"] = emails
            logger.info("Upcoming: %d scheduled Buttondown emails", len(emails))
        except Exception as exc:
            logger.error("Upcoming/Buttondown failed: %s", exc)

    # --- Buffer queued posts (GraphQL API) ---
    if buffer_token:
        try:
            gql_headers = {
                "Authorization": f"Bearer {buffer_token}",
                "Content-Type": "application/json",
            }
            # Get org ID
            r = httpx.post(
                "https://api.buffer.com",
                headers=gql_headers,
                json={"query": "{ account { organizations { id } } }"},
                timeout=30,
            )
            r.raise_for_status()
            org_id = r.json()["data"]["account"]["organizations"][0]["id"]

            # Fetch scheduled posts
            r = httpx.post(
                "https://api.buffer.com",
                headers=gql_headers,
                json={"query": f"""
                    query {{
                      posts(input: {{
                        organizationId: "{org_id}",
                        filter: {{ status: [scheduled, draft] }}
                      }}, first: 100) {{
                        edges {{
                          node {{
                            text
                            dueAt
                            status
                            channelService
                            channel {{ displayName }}
                          }}
                        }}
                      }}
                    }}
                """},
                timeout=30,
            )
            r.raise_for_status()
            edges = r.json()["data"]["posts"]["edges"]
            queued = [
                {
                    "platform": e["node"]["channelService"],
                    "account": (e["node"].get("channel") or {}).get("displayName", ""),
                    "text": e["node"].get("text", ""),
                    "scheduled_at": e["node"].get("dueAt", ""),
                    "status": e["node"].get("status", ""),
                }
                for e in edges
            ]
            sources["buffer"] = queued
            logger.info("Upcoming: %d Buffer queued posts", len(queued))
        except Exception as exc:
            logger.error("Upcoming/Buffer failed: %s", exc)

    if not sources:
        return None

    return {
        "platform": "upcoming",
        "collected_at": _iso(_utcnow()),
        "sources": sources,
    }
