from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)


def _collect_buttondown_newsletter(
    client: httpx.Client,
    newsletter_name: str,
    newsletter_key: str,
    since: datetime | None,
    use_since: bool,
) -> tuple[list[dict], int | None, dict[str, int], dict[str, int]]:
    """Collect emails and subscriber data for a single Buttondown newsletter.

    Returns (emails, total_subscriber_count, tag_totals, tag_new_since).
    tag_totals and tag_new_since are empty dicts for newsletters with no tags.
    """
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

    # Check if this newsletter has any tags before paginating all subscribers
    tags_r = client.get("https://api.buttondown.email/v1/tags", headers=headers)
    tags_r.raise_for_status()
    has_tags = tags_r.json().get("count", 0) > 0

    tag_totals: dict[str, int] = {}
    tag_new: dict[str, int] = {}
    subscriber_count: int | None = None

    if has_tags:
        # Paginate all subscribers and count by tag client-side
        # (API tag filter doesn't work server-side)
        sub_page = 1
        while True:
            sr = client.get(
                "https://api.buttondown.email/v1/subscribers",
                params={"page_size": 100, "page": sub_page},
                headers=headers,
            )
            sr.raise_for_status()
            sub_data = sr.json()
            if subscriber_count is None:
                subscriber_count = sub_data.get("count")
            for sub in sub_data.get("results", []):
                for tag in sub.get("tags", []):
                    tag_totals[tag] = tag_totals.get(tag, 0) + 1
                    count_since = since or _default_since()
                    if sub.get("creation_date"):
                        try:
                            created = datetime.fromisoformat(
                                sub["creation_date"].replace("Z", "+00:00")
                            )
                            if created >= count_since:
                                tag_new[tag] = tag_new.get(tag, 0) + 1
                        except ValueError:
                            pass
            if not sub_data.get("next"):
                break
            sub_page += 1
    else:
        sr = client.get(
            "https://api.buttondown.email/v1/subscribers",
            params={"page_size": 1},
            headers=headers,
        )
        sr.raise_for_status()
        subscriber_count = sr.json().get("count", None)

    return newsletters, subscriber_count, tag_totals, tag_new


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
        tag_counts: dict[str, dict[str, int]] = {}
        tag_new_counts: dict[str, dict[str, int]] = {}

        with httpx.Client(timeout=30) as client:
            # Discover all newsletters on the account
            r = client.get(
                "https://api.buttondown.email/v1/newsletters",
                headers={"Authorization": f"Token {api_key}"},
            )
            r.raise_for_status()
            account_newsletters = r.json().get("results", [])

            # If the account has no sub-newsletters (single-newsletter account),
            # collect directly with the provided key.
            if not account_newsletters:
                account_newsletters = [{"name": "default", "api_key": api_key}]

            for nl in account_newsletters:
                nl_name = nl.get("name") or nl.get("domain") or nl.get("id", "default")
                nl_key = nl.get("api_key", api_key)
                try:
                    emails, count, tag_totals, tag_new = _collect_buttondown_newsletter(
                        client, nl_name, nl_key, since, use_since
                    )
                    all_newsletters.extend(emails)
                    if count is not None:
                        subscriber_counts[nl_name] = count
                    if tag_totals:
                        tag_counts[nl_name] = tag_totals
                    if tag_new:
                        tag_new_counts[nl_name] = tag_new
                    logger.info(
                        "Buttondown [%s]: %d emails, %s subscribers, %d tags",
                        nl_name, len(emails), count, len(tag_totals),
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
        if tag_counts:
            result["subscriber_tags"] = tag_counts
        if tag_new_counts:
            result["new_subscribers_by_tag"] = tag_new_counts
        if use_since:
            result["since"] = _iso(since)
        return result

    except Exception as exc:
        logger.error("Buttondown collection failed: %s", exc)
        return None
