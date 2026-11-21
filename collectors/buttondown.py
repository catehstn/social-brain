from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)

# Cohort tags look like "dri-3", "em-2", "ai-2a": a course prefix and a cohort
# number. Interest tags ("dri-future", "talk") don't match. Someone holding
# cohort tags for two different courses bought more than once. Two cohorts of
# the same course ("dri-2" + "dri-3") is a transfer, not a second purchase.
_COHORT_TAG_RE = re.compile(r"^[a-z]+-\d+[a-z]?$")


def _collect_buttondown_newsletter(
    client: httpx.Client,
    newsletter_name: str,
    newsletter_key: str,
    since: datetime | None,
    use_since: bool,
) -> tuple[list[dict], int | None, dict[str, int], dict[str, int], dict[str, Any]]:
    """Collect emails and subscriber data for a single Buttondown newsletter.

    Returns (emails, total_subscriber_count, tag_totals, tag_new_since, confirmed).
    tag_totals and tag_new_since are empty dicts for newsletters with no tags.

    total_subscriber_count is the API's raw count, which includes unactivated
    (never confirmed, often bots), unsubscribed and undeliverable addresses.
    `confirmed` holds the counts that exclude them: `count`, `tags`, `new_by_tag`
    (type == "regular" only), plus `by_type`, `buyers` and `repeat_buyers`
    (any type; someone who bought and later unsubscribed still bought) and
    `transfers` (two cohorts of one course, not a second purchase).
    It is empty when the API returns no subscriber `type`.
    """
    headers = {"Authorization": f"Token {newsletter_key}"}
    newsletters: list[dict] = []
    page = 1

    while True:
        r = client.get(
            "https://api.buttondown.email/v1/emails",
            params={"status": "sent", "page_size": 20, "page": page, "ordering": "-publish_date"},
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

    tag_totals: dict[str, int] = {}
    tag_new: dict[str, int] = {}
    subscriber_count: int | None = None

    # Paginate all subscribers: tags and confirmation status are both counted
    # client-side (the API tag filter doesn't work server-side).
    by_type: dict[str, int] = {}
    confirmed_tags: dict[str, int] = {}
    confirmed_new: dict[str, int] = {}
    confirmed_count = 0
    buyers = 0
    repeat_buyers = 0
    transfers = 0
    count_since = since or _default_since()
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
            sub_type = sub.get("type")
            is_confirmed = sub_type == "regular"
            if sub_type:
                by_type[sub_type] = by_type.get(sub_type, 0) + 1
            if is_confirmed:
                confirmed_count += 1
            is_new = False
            if sub.get("creation_date"):
                try:
                    created = datetime.fromisoformat(
                        sub["creation_date"].replace("Z", "+00:00")
                    )
                    is_new = created >= count_since
                except ValueError:
                    pass
            tags = sub.get("tags") or []
            cohort_tags = [t for t in tags if _COHORT_TAG_RE.match(t)]
            if cohort_tags:
                buyers += 1
            courses = {t.rsplit("-", 1)[0] for t in cohort_tags}
            if len(courses) >= 2:
                repeat_buyers += 1
            elif len(cohort_tags) >= 2:
                transfers += 1
            for tag in tags:
                tag_totals[tag] = tag_totals.get(tag, 0) + 1
                if is_new:
                    tag_new[tag] = tag_new.get(tag, 0) + 1
                if is_confirmed:
                    confirmed_tags[tag] = confirmed_tags.get(tag, 0) + 1
                    if is_new:
                        confirmed_new[tag] = confirmed_new.get(tag, 0) + 1
        if not sub_data.get("next"):
            break
        sub_page += 1

    confirmed: dict[str, Any] = {}
    if by_type:
        confirmed = {
            "count": confirmed_count,
            "by_type": by_type,
            "tags": confirmed_tags,
            "new_by_tag": confirmed_new,
            "buyers": buyers,
            "repeat_buyers": repeat_buyers,
            "transfers": transfers,
        }

    return newsletters, subscriber_count, tag_totals, tag_new, confirmed


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
        confirmed_by_nl: dict[str, dict[str, Any]] = {}

        with httpx.Client(timeout=30) as client:
            # Discover all newsletters on the account
            r = client.get(
                "https://api.buttondown.email/v1/newsletters",
                headers={"Authorization": f"Token {api_key}"},
            )
            r.raise_for_status()
            account_newsletters = r.json().get("results", [])

            if not account_newsletters:
                account_newsletters = [{"name": "default", "api_key": api_key}]

            for nl in account_newsletters:
                # `.get(key, default)` only fires the default on missing key —
                # a payload with `"name": null` would still return None. Final
                # `or "default"` catches that and the empty-string case.
                nl_name = nl.get("name") or nl.get("domain") or nl.get("id") or "default"
                nl_key = nl.get("api_key", api_key)
                try:
                    emails, count, tag_totals, tag_new, confirmed = _collect_buttondown_newsletter(
                        client, nl_name, nl_key, since, use_since
                    )
                    all_newsletters.extend(emails)
                    if count is not None:
                        subscriber_counts[nl_name] = count
                    if tag_totals:
                        tag_counts[nl_name] = tag_totals
                    if tag_new:
                        tag_new_counts[nl_name] = tag_new
                    if confirmed:
                        confirmed_by_nl[nl_name] = confirmed
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
        if confirmed_by_nl:
            # Use these for list size and growth, not subscriber_counts.
            result["confirmed_subscribers"] = confirmed_by_nl
        if use_since:
            result["since"] = _iso(since)
        return result

    except Exception as exc:
        logger.error("Buttondown collection failed: %s", exc)
        return None
