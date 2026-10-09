from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)


# The question text Calendly's form uses varies — "How did you hear about me?",
# "How did you find me?", "What brought you here?". Pin the start of the
# question so a general-info prompt like "Any questions you want me to hear
# about before the call?" or "Did you find me helpful?" is NOT picked up as
# an attribution question — the previous looser patterns pulled unrelated
# answers through `_classify_attribution` and poisoned the rollup.
_ATTRIBUTION_QUESTION_PATTERNS = [
    re.compile(r"^\s*(how|where)\s+did\s+you\s+hear\s+about\b", re.IGNORECASE),
    re.compile(r"^\s*(how|where)\s+did\s+you\s+find\s+", re.IGNORECASE),
    re.compile(r"^\s*what\s+brought\s+you\b", re.IGNORECASE),
    re.compile(r"^\s*who\s+referred\s+you\b", re.IGNORECASE),
    re.compile(r"^\s*referred\s+by\b", re.IGNORECASE),
]


def _is_attribution_question(question: str | None) -> bool:
    if not question:
        return False
    return any(p.search(question) for p in _ATTRIBUTION_QUESTION_PATTERNS)


# Word-boundary patterns for named people. A bare `in` check mis-bucketed
# "educator", "educated", "communicate", "dedicate" and "jeans" to Cate/Jean.
# No IGNORECASE: `_classify_attribution` lowercases the input first.
_CATE_NAME = re.compile(r"\bcate\b")
_JEAN_NAME = re.compile(r"\bjean\b")


def _classify_attribution(answer: str | None) -> str:
    """Group a free-text referral answer ('How did you hear about me?') into a
    channel.

    Platform/channel markers are checked before named people, so that
    'Cate's LinkedIn' buckets to LinkedIn (the channel), not Cate. Within
    the channel group O'Reilly is checked before Newsletter so
    'O'Reilly newsletter' doesn't lose the course-side origin.

    No PII leaves this function. Only the channel label is returned —
    never the raw answer, invitee name or email.
    """
    if not answer or not answer.strip():
        return "Unknown"
    # Normalise curly apostrophes so 'O’Reilly' matches the ASCII pattern.
    s = answer.replace("’", "'").lower()
    # Channels first (O'Reilly before Newsletter: an O'Reilly newsletter
    # answer is a course-side origin, not generic newsletter).
    #
    # "LinkedIn Learning" is a different beast — a course-adjacent LMS,
    # not the social network. Course-side bucket (grouped with O'Reilly
    # for the growth-channel analysis) wins.
    if "linkedin learning" in s:
        return "O'Reilly"
    if "linkedin" in s:
        return "LinkedIn"
    if "o'reilly" in s or "oreilly" in s:
        return "O'Reilly"
    if "newsletter" in s or "buttondown" in s or "substack" in s:
        return "Newsletter"
    # Named referrers — word-boundary matched so "educator", "advocate",
    # "dedicate", "jeans" etc. don't false-match.
    if _CATE_NAME.search(s):
        return "Cate"
    if _JEAN_NAME.search(s):
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

    For a group event this returns the first invitee's channel; group
    events are rare for 1:1 coaching but note it as a design limit (#57).
    """
    if not event_uri:
        # The event payload lacked a `uri` — can't ask for its invitees.
        return None
    try:
        r = client.get(f"{event_uri}/invitees")
        r.raise_for_status()
        for invitee in r.json().get("collection", []):
            # `.get("x", [])` default only fires on a missing key, not a
            # null value — Calendly returns `questions_and_answers: null`
            # when there are no questions, which would raise TypeError.
            for qa in (invitee.get("questions_and_answers") or []):
                if _is_attribution_question(qa.get("question")):
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

            # One invitees call per active event. Accumulate directly into
            # the rollup — per-booking records would be redundant with the
            # grouped values and the issue's PII guidance ("grouped channel
            # values, not invitee names or emails") would need extra care
            # for every added field.
            per_event_channels: list[str | None] = [
                _fetch_event_attribution(client, event.get("uri", ""))
                for event in active_events
            ]

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

        # Rollup — grouped channel values only, no PII. None (invitees
        # fetch failed) and the "Unknown" sentinel share the same bucket:
        # the booking exists but we can't attribute it. Keeping them
        # combined keeps the sum equal to total_bookings regardless of
        # API hiccups.
        attribution_by_channel: dict[str, int] = {}
        for channel in per_event_channels:
            key = channel or "Unknown"
            attribution_by_channel[key] = attribution_by_channel.get(key, 0) + 1

        result: dict[str, Any] = {
            "platform": "calendly",
            "collected_at": _iso(_utcnow()),
            "period_start": _iso(since),
            "period_end": _iso(now),
            "total_bookings": total_active,
            "total_canceled": total_canceled,
            "bookings_by_type": bookings_by_type,
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
