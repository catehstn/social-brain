"""
analyse.py — builds the analysis prompt from collected platform data and
writes it to reports/prompt-YYYY-WNN.txt for manual use with claude.ai.

The prompt asks Claude for two outputs:
  1. A markdown performance report
  2. A filled viz/data.js for the analytics dashboard
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

# Path to the dashboard data template — embedded in the prompt so Claude knows
# exactly what schema to fill.
VIZ_DIR = Path(__file__).parent / "viz"
DATA_TEMPLATE_PATH = VIZ_DIR / "data.template.js"

# ---------------------------------------------------------------------------
# Prompt parts  (split so data.template.js can be inserted without escaping
# every JS brace as {{ / }} for Python's str.format)
# ---------------------------------------------------------------------------

_PREAMBLE = """\
The following message contains pre-collected analytics data in JSON format. \
All data has already been gathered — you do not need to make any API calls \
or access any external services. Your only task is to read the data and \
produce the report and dashboard data file described below.

You are a content analytics expert. Your analysis must be grounded entirely \
in the data provided. Do not invent numbers, do not speculate beyond what \
the data supports, and do not pad with generic advice. Every recommendation \
must be tied to a specific observed signal.

Only analyse the platforms listed under "Data collected from". Do not \
reference, speculate about, or leave placeholder sections for any platform \
not in that list.

---

Analyse the following data and produce two outputs:

## OUTPUT 1: Markdown report

Title the report:
# {period_window} Content Performance Report — {period_id}

Include exactly these sections:

### Data collected from
List only the platforms present in the JSON.

### 1. What Worked
Top-performing content. For each item, explain *why* it likely performed \
well based on the data — engagement type, topic, format, timing. Do not \
list items without a reason.

### 2. What Didn't
Underperforming posts or patterns. Brief hypothesis for each. Skip this \
section if there is genuinely nothing to note.

### 3. Cross-Platform Patterns
Only include if data from two or more platforms is present. Look for topics, \
formats, timing, or audience behaviours that appear consistently across \
platforms. If referrer or mention data is present, note which content or \
topics drove inbound traffic or external discussion. Omit if only one \
platform has data.

### 4. Next Period Suggestions
Exactly 5 specific content ideas. For each:
- The idea (1–2 sentences)
- Recommended platform(s) — only suggest platforms in the data
- The signal that justifies it

If upcoming scheduled content is listed in the data, treat those as already \
planned and suggest complementary ideas rather than duplicating them.

### 5. Metrics Summary
A single markdown table, one row per platform. Include only platforms in \
the data. Use "n/a" for fields not available in that platform's data. \
For mentions, summarise total HN hits, Mastodon mentions, and Bluesky \
mentions in one row. For jetpack referrers, list the top 3 sources.

---

## OUTPUT 2: viz/data.js

Produce a filled data.js by copying the template below and replacing every \
REPLACE_WITH_* placeholder with real values from the JSON.

Rules:
- Do not invent numbers. If a value is genuinely unavailable, use 0 or \
"n/a" and add a comment explaining why.
- Only include tabs/sections for platforms that have data. If a platform is \
missing, set its arrays to [] and its STATS entries to value: "n/a".
- blogDaily, linkedinDaily, vercelDaily: include all days in the period. \
Use {{d: "Mon DD", ...}} format.
- mastodonPosts: top 9 by total engagement (fav + boost + reply). If fewer \
than 9 posts, include all.
- vercelDaily / vercelReferrers: if Vercel analytics were not running for \
the full period, add a comment noting the actual start date.
- monthlyFunnel: aggregate daily data by calendar month. If the period ends \
mid-month, label that entry "Mon (partial)".
- funnelInsights: write exactly 4 items grounded in what the data shows for \
this specific period. Each must cite a specific number or pattern.
- Do not touch Dashboard.jsx.

data.template.js:

"""

_OUTPUT2_SUFFIX = """

---

## Configuration

PERIOD_ID: {period_id}
PERIOD_WINDOW: {period_window}
DATE_RANGE: {date_range}

Data collected from:
{platforms_available}

Data shape notes (for interpreting the JSON):
- mastodon / bluesky: posts sorted by engagement; each post has content/text, \
favourites/likes, boosts/reposts, replies, has_attachment, attachment_types
- linkedin: daily_engagement (date, impressions, engagements, new_followers); \
top_posts_by_engagement and top_posts_by_impressions each include post text \
scraped from the public URL
- jetpack: daily_views, top_posts (views this period), referrers (traffic \
sources aggregated across the period, sorted by views)
- buttondown: subscriber_counts per newsletter; newsletters list with \
open_rate, click_rate, unsubscribes per issue
- vercel: daily_views, visitors, top_pages, referrers, bounce_rate
- amazon: by_marketplace dict — each marketplace has a list of book editions \
with best_sellers_rank, rating, reviews
- upcoming: sources.wordpress (scheduled blog posts with title, date, content), \
sources.buttondown (scheduled emails), sources.buffer (queued social posts \
with platform, text, scheduled_at)
- mentions: sources.hacker_news (stories/comments containing monitored domains, \
with points and num_comments); sources.mastodon / sources.bluesky (@ mention \
notifications); sources.google_search_console (top queries and pages by clicks)

Goals for this period:
{goals}

Content pillars:
{pillars}

{upcoming_section}---

DATA (JSON):
{data_json}
"""


def _read_viz_template() -> str:
    """Return the contents of viz/data.template.js, or a placeholder if missing."""
    if DATA_TEMPLATE_PATH.exists():
        return DATA_TEMPLATE_PATH.read_text()
    logger.warning("viz/data.template.js not found at %s — OUTPUT 2 will be incomplete", DATA_TEMPLATE_PATH)
    return "// data.template.js not found — see viz/data.template.js in the repo\n"


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

    # Mastodon: strip HTML, truncate, drop non-analytics fields, keep top 30 by engagement
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
        data["mastodon"]["posts"] = mastodon_posts[:30]
        data["mastodon"]["note"] = f"Showing top 30 of {len(mastodon_posts)} posts by engagement"

    # Bluesky: truncate text, drop URI, keep top 30 by engagement
    bluesky_posts = data.get("bluesky", {}).get("posts", [])
    for post in bluesky_posts:
        post["text"] = post.get("text", "")[:200]
        post.pop("uri", None)
    bluesky_posts.sort(
        key=lambda p: p.get("likes", 0) + p.get("reposts", 0) + p.get("replies", 0),
        reverse=True,
    )
    if "bluesky" in data:
        data["bluesky"]["posts"] = bluesky_posts[:30]
        data["bluesky"]["note"] = f"Showing top 30 of {len(bluesky_posts)} posts by engagement"

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

    trimmed = _trim_data(collected_data)
    data_json = json.dumps(trimmed, indent=2, default=str)
    upcoming_section = _format_upcoming_section(collected_data)

    # --- Assemble prompt (template JS inserted as plain string to avoid brace conflicts) ---
    preamble = _PREAMBLE.format(
        period_window=period_window,
        period_id=period,
    )
    template_js = _read_viz_template()
    suffix = _OUTPUT2_SUFFIX.format(
        period_id=period,
        period_window=period_window,
        date_range=date_range,
        platforms_available=platforms_available,
        goals=goals,
        pillars=pillars,
        upcoming_section=upcoming_section,
        data_json=data_json,
    )

    return preamble + template_js + suffix


def save_prompt(
    collected_data: dict[str, Any],
    config: dict[str, Any],
    period: str,
    reports_dir: Path,
    months: int | None = None,
) -> Path:
    """
    Build the analysis prompt and write it to reports/prompt-{period}.txt.
    Returns the path to the written file.
    """
    prompt = build_prompt(collected_data, config, period, months=months)
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / f"prompt-{period}.txt"
    path.write_text(prompt)
    logger.info("Prompt saved → %s (%d chars)", path, len(prompt))
    return path
