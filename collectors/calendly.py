from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)


_ATTRIBUTION_QUESTION_HINT = re.compile(r"\bhear\b.*\babout\b", re.IGNORECASE)


def _classify_attribution(answer: str | None) -> str:
    """Group a free-text referral answer ('How did you hear about me?') into a
    channel.

    Platform/channel markers are checked before named people, so that
    'Cate's LinkedIn' buckets to LinkedIn (the channel), not Cate. This
    mirrors how dri_sales.py groups the Stripe answers so coaching and
    course attribution read on the same axis (#57).

    No PII leaves this function. Only the channel label is returned —
    never the raw answer, invitee name or email.
    """
    if not answer:
        return "Unknown"
    s = answer.lower()
    # Channels first.
    if "linkedin" in s:
        return "LinkedIn"
    if "newsletter" in s or "buttondown" in s or "substack" in s:
        return "Newsletter"
    if "o'reilly" in s or "oreilly" in s or "o reilly" in s:
        return "O'Reilly"
    # Named referrers.
    if "cate" in s:
        return "Cate"
    if "jean" in s:
        return "Jean"
    # Explicit catch-all for conversational referrals.
    if (
        "word of mouth" in s
        or "friend" in s
        or "colleague" in s
        or "recommend" in s
        or "referral" in s
    ):
        return "Word of mouth"
    return "Other"


def _fetch_event_attribution(client: httpx.Client, event_uri: str) -> str | None:
    """Return the attribution channel for a single scheduled event.

    Returns 'Unknown' when the invitees API returned successfully but
    carried no attribution answer. Returns None on a transport error so
    the caller can distinguish a missing-signal booking from a known
    'no referral answer' one.
    """
    try:
        r = client.get(f"{event_uri}/invitees")
        r.raise_for_status()
        for invitee in r.json().get("collection", []):
            for qa in invitee.get("questions_and_answers", []):
                if _ATTRIBUTION_QUESTION_HINT.search(qa.get("question", "") or ""):
                    return _classify_attribution(qa.get("answer"))
    except Exception as exc:
        logger.warning("Calendly invitees fetch failed for %s: %s", event_uri, exc)
        return None
    return "Unknown"


def collect_calendly(
    token: str,
    since: datetime | None = None,
    lead_gen_event: str | None = None,
) -> dict[str, Any] | None:
    """
    Collect booking data from Calendly as a lead-gen metric.
    Returns bookings grouped by event type with active/cancelled counts.
    If lead_gen_event is set, the active count for that event type is
    surfaced separately as lead_gen_bookings.
    Requires a Personal Access Token from calendly.com/integrations/api_webhooks.
    """
    if since is None:
        since = _default_since()

    base = "https://api.calendly.com"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        with httpx.Client(timeout=30, headers=headers) as client:
            # Resolve current user URI
            r = client.get(f"{base}/users/me")
            r.raise_for_status()
            user_uri = r.json()["resource"]["uri"]

            # Fetch event types (to map URIs → friendly names)
            r = client.get(f"{base}/event_types", params={"user": user_uri, "count": 100})
            r.raise_for_status()
            event_types_raw = r.json().get("collection", [])
            event_type_names: dict[str, str] = {
                et["uri"]: et["name"] for et in event_types_raw
            }

            # Fetch scheduled events within the lookback window.
            # Active events: no max_start_time so upcoming booked sessions are included.
            # Canceled events: bounded by now (only past cancellations are relevant).
            now = _utcnow()
            r = client.get(
                f"{base}/scheduled_events",
                params={
                    "user": user_uri,
                    "min_start_time": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "count": 100,
                    "status": "active",
                },
            )
            r.raise_for_status()
            active_events = r.json().get("collection", [])

            r = client.get(
                f"{base}/scheduled_events",
                params={
                    "user": user_uri,
                    "min_start_time": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "max_start_time": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "count": 100,
                    "status": "canceled",
                },
            )
            r.raise_for_status()
            canceled_events = r.json().get("collection", [])

            # Build the per-booking attribution list — one invitees call per
            # active event. Canceled events are counted but not attributed:
            # a cancellation doesn't change where the lead came from, and the
            # cost scales with the active window.
            def _event_name(event: dict) -> str:
                et_uri = event.get("event_type", "")
                return event_type_names.get(et_uri, et_uri.split("/")[-1])

            if lead_gen_event:
                active_events = [e for e in active_events if _event_name(e) == lead_gen_event]
                canceled_events = [e for e in canceled_events if _event_name(e) == lead_gen_event]

            bookings: list[dict[str, Any]] = []
            for event in active_events:
                channel = _fetch_event_attribution(client, event.get("uri", ""))
                bookings.append({
                    "event_type": _event_name(event),
                    "scheduled_at": event.get("start_time"),
                    "attribution_channel": channel,
                })

        # Aggregate by event type
        by_type: dict[str, dict[str, int]] = {}
        for event in active_events:
            name = _event_name(event)
            by_type.setdefault(name, {"active": 0, "canceled": 0})["active"] += 1
        for event in canceled_events:
            name = _event_name(event)
            by_type.setdefault(name, {"active": 0, "canceled": 0})["canceled"] += 1

        bookings_by_type = [
            {"event_type": name, **counts}
            for name, counts in sorted(by_type.items())
        ]
        total_active = sum(e["active"] for e in bookings_by_type)
        total_canceled = sum(e["canceled"] for e in bookings_by_type)

        # Rollup — grouped channel values only, no PII.
        attribution_by_channel: dict[str, int] = {}
        for b in bookings:
            # None (invitees fetch failed) and the "Unknown" sentinel share the
            # same bucket in the rollup — the booking exists but we can't
            # attribute it. Keeping them combined keeps the sum of the rollup
            # equal to total_bookings regardless of API hiccups.
            ch = b["attribution_channel"] or "Unknown"
            attribution_by_channel[ch] = attribution_by_channel.get(ch, 0) + 1

        result: dict[str, Any] = {
            "platform": "calendly",
            "collected_at": _iso(_utcnow()),
            "period_start": _iso(since),
            "period_end": _iso(now),
            "total_bookings": total_active,
            "total_canceled": total_canceled,
            "bookings_by_type": bookings_by_type,
            "bookings": bookings,
            "attribution_by_channel": attribution_by_channel,
        }

        if lead_gen_event:
            result["lead_gen_event"] = lead_gen_event
            match = next((e for e in bookings_by_type if e["event_type"] == lead_gen_event), None)
            result["lead_gen_bookings"] = match["active"] if match else 0

        return result
    except Exception as exc:
        logger.error("Calendly collection failed: %s", exc)
        return None
