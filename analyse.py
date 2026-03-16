"""
analyse.py — builds the analysis prompt from collected platform data and
writes it to reports/prompt-YYYY-WNN.txt for manual use with claude.ai.

The prompt asks Claude for two outputs:
  1. A markdown performance report
  2. A self-contained React artifact that renders the analytics dashboard
     inline in claude.ai (no local server needed)
"""

from __future__ import annotations

import copy
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Prompt template files — edit these to customise the report/artifact instructions.
PROMPTS_DIR = Path(__file__).parent / "prompts"
PREAMBLE_PATH = PROMPTS_DIR / "preamble.txt"
SUFFIX_PATH = PROMPTS_DIR / "suffix.txt"
UPDATE_PATH = PROMPTS_DIR / "update.txt"

# Dashboard reference component — embedded in the prompt so Claude knows the
# exact structure, charts, tabs, and styling to reproduce as a self-contained artifact.
VIZ_DIR = Path(__file__).parent / "viz"
DASHBOARD_PATH = VIZ_DIR / "Dashboard.jsx"

# ---------------------------------------------------------------------------
# Prompt parts  (split so Dashboard.jsx can be inserted without escaping
# every JS brace as {{ / }} for Python's str.format)
# ---------------------------------------------------------------------------


def _read_preamble() -> str:
    """Return the prompt preamble from prompts/preamble.txt."""
    if PREAMBLE_PATH.exists():
        return PREAMBLE_PATH.read_text()
    logger.warning("prompts/preamble.txt not found — prompt will be incomplete")
    return ""


def _read_suffix() -> str:
    """Return the prompt suffix template from prompts/suffix.txt."""
    if SUFFIX_PATH.exists():
        return SUFFIX_PATH.read_text()
    logger.warning("prompts/suffix.txt not found — prompt will be incomplete")
    return "{data_json}"


def _read_dashboard_reference() -> str:
    """Return the contents of viz/Dashboard.jsx, or a placeholder if missing."""
    if DASHBOARD_PATH.exists():
        return DASHBOARD_PATH.read_text()
    logger.warning("viz/Dashboard.jsx not found at %s — OUTPUT 2 will be incomplete", DASHBOARD_PATH)
    return "// Dashboard.jsx not found — see viz/Dashboard.jsx in the repo\n"


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode common entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = (
        text.replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", '"')
            .replace("&#39;", "'")
            .replace("&nbsp;", " ")
            .replace("&#8217;", "'")
            .replace("&#8220;", '"')
            .replace("&#8221;", '"')
    )
    return re.sub(r"\s+", " ", text).strip()


def _trim_data(data: dict[str, Any]) -> dict[str, Any]:
    """
    Return a copy of collected data with HTML stripped from post content
    and long text fields truncated, to keep the prompt a manageable size.
    """
    data = copy.deepcopy(data)

    # Mastodon: strip HTML, truncate, drop non-analytics fields, keep top 15 by engagement
    mastodon_posts = data.get("mastodon", {}).get("posts", [])
    for post in mastodon_posts:
        post["content"] = _strip_html(post.get("content", ""))[:200]
        post.pop("id", None)
        post.pop("url", None)
    mastodon_posts.sort(
        key=lambda p: p.get("favourites", 0) + p.get("boosts", 0) + p.get("replies", 0),
        reverse=True,
    )
    if "mastodon" in data:
        data["mastodon"]["posts"] = mastodon_posts[:15]
        data["mastodon"]["note"] = f"Showing top 15 of {len(mastodon_posts)} posts by engagement"

    # Bluesky: truncate text, drop URI, keep top 15 by engagement
    bluesky_posts = data.get("bluesky", {}).get("posts", [])
    for post in bluesky_posts:
        post["text"] = post.get("text", "")[:200]
        post.pop("uri", None)
    bluesky_posts.sort(
        key=lambda p: p.get("likes", 0) + p.get("reposts", 0) + p.get("replies", 0),
        reverse=True,
    )
    if "bluesky" in data:
        data["bluesky"]["posts"] = bluesky_posts[:15]
        data["bluesky"]["note"] = f"Showing top 15 of {len(bluesky_posts)} posts by engagement"

    # LinkedIn: truncate post text, deduplicate the two top-post lists, cap daily data
    if "linkedin" in data:
        li = data["linkedin"]

        # Truncate post text and keep top 15 by engagement; drop impressions list (overlaps heavily)
        eng_posts = li.get("top_posts_by_engagement", [])
        for post in eng_posts:
            post["text"] = _strip_html(post.get("text", ""))[:300]
            post.pop("url", None)
        li["top_posts_by_engagement"] = eng_posts[:15]
        li["top_posts_by_impressions_note"] = "Omitted — see top_posts_by_engagement for post content"
        li.pop("top_posts_by_impressions", None)

        # Cap daily engagement to 30 most recent days
        daily = li.get("daily_engagement", [])
        if len(daily) > 30:
            li["daily_engagement"] = daily[-30:]
            li["daily_engagement_note"] = f"Showing most recent 30 of {len(daily)} days"

        # Drop demographics — not needed for content analysis
        li.pop("demographics", None)

    # Buttondown: drop full body — not useful for analytics
    for email in data.get("buttondown", {}).get("newsletters", []):
        email.pop("body", None)
        email.pop("id", None)

    # Vercel: keep most recent 30 days if period is long
    if "vercel" in data:
        daily = data["vercel"].get("daily_views", [])
        if len(daily) > 30:
            data["vercel"]["daily_views"] = daily[-30:]
            data["vercel"]["daily_views_note"] = f"Showing most recent 30 of {len(daily)} days"

    # Upcoming: truncate WordPress post content (can be very long)
    for post in data.get("upcoming", {}).get("sources", {}).get("wordpress", []):
        post["content"] = post.get("content", "")[:500]

    return data


def _format_upcoming_section(data: dict[str, Any]) -> str:
    """Return a formatted upcoming content block for the prompt, or empty string."""
    upcoming = data.get("upcoming", {})
    if not upcoming:
        return ""

    lines = ["Upcoming scheduled content (already planned — inform section 4):"]
    sources = upcoming.get("sources", {})

    wp_posts = sources.get("wordpress", [])
    if wp_posts:
        lines.append("\nWordPress (scheduled blog posts):")
        for p in wp_posts:
            date = p.get("scheduled_date", "")[:10]
            lines.append(f"  - {date}: {_strip_html(p.get('title', ''))}")

    bd_emails = sources.get("buttondown", [])
    if bd_emails:
        lines.append("\nButtondown (scheduled newsletters):")
        for e in bd_emails:
            date = (e.get("scheduled_date") or "")[:10]
            lines.append(f"  - {date}: {e.get('subject', '')}")

    buffer_posts = sources.get("buffer", [])
    if buffer_posts:
        lines.append("\nBuffer (queued social posts):")
        for p in buffer_posts:
            platform = p.get("platform", "")
            due = (p.get("scheduled_at") or "")[:16].replace("T", " ")
            text = (p.get("text") or "")[:100]
            lines.append(f"  - [{platform}] {due}: {text}{'…' if len(p.get('text',''))>100 else ''}")

    if len(lines) == 1:
        return ""  # only header, no actual content

    return "\n".join(lines) + "\n\n"


def build_prompt(
    collected_data: dict[str, Any],
    config: dict[str, Any],
    period: str,
    months: int | None = None,
) -> str:
    # --- Computed values ---
    if months:
        period_window = f"{months}-Month"
        suggestions_heading = "Next Period Suggestions"
        goals_heading = "Goals"
    else:
        period_window = "Weekly"
        suggestions_heading = "Next Week Suggestions"
        goals_heading = "Weekly goals"

    # Date range: derive from collected_at and since fields in the data
    collected_at = ""
    since_date = ""
    for platform_data in collected_data.values():
        if isinstance(platform_data, dict):
            if not collected_at and platform_data.get("collected_at"):
                collected_at = platform_data["collected_at"][:10]
            if not since_date and platform_data.get("since"):
                since_date = platform_data["since"][:10]

    if not collected_at:
        collected_at = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _fmt_date(iso: str) -> str:
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
            return dt.strftime("%b %-d %Y")
        except Exception:
            return iso

    if since_date:
        date_range = f"{_fmt_date(since_date)} – {_fmt_date(collected_at)}"
    else:
        date_range = collected_at

    platforms_available = (
        "\n".join(f"- {p}" for p in collected_data if p != "upcoming")
        if collected_data
        else "- (no data collected)"
    )

    pillars = "\n".join(
        f"- {p}" for p in config.get("content_pillars", [])
    ) or "- (none specified)"

    goals = "\n".join(
        f"- {g}" for g in config.get("weekly_goals", [])
    ) or "- (none specified)"

    primary_focus = config.get("primary_focus", "") or "(none specified)"

    trimmed = _trim_data(collected_data)
    data_json = json.dumps(trimmed, indent=2, default=str)
    upcoming_section = _format_upcoming_section(collected_data)

    # --- Assemble prompt ---
    preamble = _read_preamble().format(
        period_window=period_window,
        period_id=period,
    )
    suffix = _read_suffix().format(
        period_id=period,
        period_window=period_window,
        date_range=date_range,
        platforms_available=platforms_available,
        primary_focus=primary_focus,
        goals=goals,
        pillars=pillars,
        upcoming_section=upcoming_section,
        data_json=data_json,
    )

    return preamble + suffix


def build_update_prompt(
    collected_data: dict[str, Any],
    config: dict[str, Any],
    period: str,
    months: int | None = None,
) -> str:
    """
    Build a compact follow-up prompt for use in the same claude.ai chat as the
    original report. Instructs Claude to update the existing report and dashboard
    in-place rather than producing a full new analysis from scratch.
    """
    if not UPDATE_PATH.exists():
        logger.warning("prompts/update.txt not found — falling back to full prompt")
        return build_prompt(collected_data, config, period, months=months)

    period_window = f"{months}-Month" if months else "Weekly"

    collected_at = ""
    since_date = ""
    for platform_data in collected_data.values():
        if isinstance(platform_data, dict):
            if not collected_at and platform_data.get("collected_at"):
                collected_at = platform_data["collected_at"][:10]
            if not since_date and platform_data.get("since"):
                since_date = platform_data["since"][:10]
    if not collected_at:
        collected_at = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _fmt_date(iso: str) -> str:
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
            return dt.strftime("%b %-d %Y")
        except Exception:
            return iso

    date_range = f"{_fmt_date(since_date)} – {_fmt_date(collected_at)}" if since_date else collected_at
    platforms_available = (
        "\n".join(f"- {p}" for p in collected_data if p != "upcoming")
        if collected_data else "- (no data collected)"
    )
    primary_focus = config.get("primary_focus", "") or "(none specified)"
    upcoming_section = _format_upcoming_section(collected_data)
    trimmed = _trim_data(collected_data)
    data_json = json.dumps(trimmed, indent=2, default=str)

    return UPDATE_PATH.read_text().format(
        period_id=period,
        period_window=period_window,
        date_range=date_range,
        platforms_available=platforms_available,
        primary_focus=primary_focus,
        upcoming_section=upcoming_section,
        data_json=data_json,
    )


def save_prompt(
    collected_data: dict[str, Any],
    config: dict[str, Any],
    period: str,
    reports_dir: Path,
    months: int | None = None,
    update: bool = False,
) -> Path:
    """
    Build the analysis prompt and write it to reports/.
    If update=True, generates a compact follow-up prompt (prompt-{period}-update.txt)
    for use in the same claude.ai chat as the original report.
    Returns the path to the written file.
    """
    if update:
        prompt = build_update_prompt(collected_data, config, period, months=months)
        filename = f"prompt-{period}-update.txt"
    else:
        prompt = build_prompt(collected_data, config, period, months=months)
        filename = f"prompt-{period}.txt"
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / filename
    path.write_text(prompt)
    logger.info("Prompt saved → %s (%d chars)", path, len(prompt))
    return path
