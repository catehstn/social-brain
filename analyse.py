"""
analyse.py — builds the analysis prompt from collected platform data and
writes it to reports/prompt-YYYY-WNN.txt for manual use with claude.ai.
"""

from __future__ import annotations

import copy
import json
import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

REPORT_TEMPLATE = """\
The following message contains pre-collected social media and content analytics \
data in JSON format. All data has already been gathered — you do not need to \
make any API calls or access any external services. Your only task is to read \
the data below and write the report.

You are a social media and content analytics expert. Your analysis must be \
grounded entirely in the data provided — do not invent numbers, do not \
speculate beyond what the data supports, and do not pad the report with generic \
advice. Every recommendation must be tied to a specific observed signal.

Only analyse the platforms listed under "Data collected from". \
Do not reference, speculate about, or leave placeholder sections for any \
platform not in that list. If only one platform has data, analyse only that \
platform — do not invent cross-platform comparisons.

---

Analyse the following data covering {period_description} and produce a markdown \
report with exactly these sections:

# {report_title}

## Data collected from
{platforms_available}

## 1. What Worked
Top-performing content across the platforms listed above. For each item \
explain *why* it likely performed well based on the data (engagement type, \
topic, format).

## 2. What Didn't
Underperforming posts or patterns. Offer a brief hypothesis for each.

## 3. Cross-Platform Patterns
Only include this section if data from two or more platforms is available. \
Look for topics, formats, posting times, or audience behaviours that show up \
consistently across them. If only one platform has data, omit this section.

## 4. {suggestions_heading}
Exactly 5 specific content ideas, each with:
- The idea itself (1–2 sentences)
- Recommended platform(s) — only suggest platforms in the list above
- The data signal that justifies it

## 5. Metrics Summary
A single markdown table with one row per platform (only the platforms listed \
above). Include the most important numbers for each. Use "n/a" for \
unavailable fields.

---

{goals_heading}:
{goals}

Content pillars to keep in mind:
{pillars}

---

DATA (JSON):
{data_json}
"""


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

    # Vercel: drop raw daily_views if there are many — keep summary stats and top pages
    if "vercel" in data:
        daily = data["vercel"].get("daily_views", [])
        if len(daily) > 30:
            data["vercel"]["daily_views"] = daily[-30:]  # keep most recent 30 days
            data["vercel"]["daily_views_note"] = f"Showing most recent 30 of {len(daily)} days"

    return data


def build_prompt(
    collected_data: dict[str, Any],
    config: dict[str, Any],
    period: str,
    months: int | None = None,
) -> str:
    platforms_available = (
        "\n".join(f"- {p}" for p in collected_data)
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

    if months:
        period_description = f"the past {months} months"
        report_title = f"{months}-Month Content Performance Report — {period}"
        suggestions_heading = "Next Period Suggestions"
        goals_heading = "Goals"
    else:
        period_description = "the past two weeks"
        report_title = f"Weekly Content Performance Report — {period}"
        suggestions_heading = "Next Week Suggestions"
        goals_heading = "Weekly goals"

    return REPORT_TEMPLATE.format(
        period_description=period_description,
        report_title=report_title,
        platforms_available=platforms_available,
        suggestions_heading=suggestions_heading,
        goals_heading=goals_heading,
        goals=goals,
        pillars=pillars,
        data_json=data_json,
    )


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
