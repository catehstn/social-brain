"""
analyse.py — builds the analysis prompt from collected platform data and
writes it to reports/prompt-YYYY-WNN.txt for manual use with claude.ai.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

REPORT_TEMPLATE = """\
You are a social media and content analytics expert. You receive raw engagement
data from multiple platforms and produce a concise, actionable {report_noun} report
in well-structured markdown.

Your analysis must be grounded entirely in the data provided — do not invent
numbers, do not speculate beyond what the data supports, and do not pad the
report with generic advice. Every recommendation must be tied to a specific
observed signal.

When platform data is missing or marked as unavailable, note it briefly and
move on — do not dwell on it.

---

You have been given the following social media and content analytics data for
{period_description}. Use it to produce a markdown report with exactly these
sections:

# {report_title}

## Platform data provided
{platforms_available}

## 1. What Worked
Top-performing content across all platforms. For each item explain *why* it
likely performed well based on the data (engagement type, topic, format).

## 2. What Didn't
Underperforming posts or patterns. Offer a brief hypothesis for each.

## 3. Cross-Platform Patterns
Signals that appear across more than one platform — topics, formats, posting
times, or audience behaviours that show up consistently.

## 4. {suggestions_heading}
Exactly 5 specific content ideas, each with:
- The idea itself (1–2 sentences)
- Recommended platform(s)
- The data signal that justifies it

## 5. Metrics Summary

A single markdown table with one row per platform containing the most
important numbers for that platform. Include a "n/a" for unavailable fields.

---

{goals_heading}:
{goals}

Content pillars to keep in mind:
{pillars}

---

RAW DATA (JSON):
{data_json}
"""


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

    data_json = json.dumps(collected_data, indent=2, default=str)

    if months:
        report_noun = f"{months}-month"
        period_description = f"the past {months} months"
        report_title = f"{months}-Month Content Performance Report — {period}"
        suggestions_heading = "Next Period Suggestions"
        goals_heading = "Goals"
    else:
        report_noun = "weekly"
        period_description = "the past two weeks"
        report_title = f"Weekly Content Performance Report — {period}"
        suggestions_heading = "Next Week Suggestions"
        goals_heading = "Weekly goals"

    return REPORT_TEMPLATE.format(
        report_noun=report_noun,
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
    logger.info("Prompt saved → %s", path)
    return path
