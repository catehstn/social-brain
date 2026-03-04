"""
analyse.py — sends collected platform data to the Claude API and returns
a structured markdown report.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import anthropic

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-20250514"

SYSTEM_PROMPT = """\
You are a social media and content analytics expert. You receive raw engagement
data from multiple platforms and produce a concise, actionable weekly report
in well-structured markdown.

Your analysis must be grounded entirely in the data provided — do not invent
numbers, do not speculate beyond what the data supports, and do not pad the
report with generic advice. Every recommendation must be tied to a specific
observed signal.

When platform data is missing or marked as unavailable, note it briefly and
move on — do not dwell on it.
"""

REPORT_TEMPLATE = """\
You have been given the following social media and content analytics data for
the past week/two weeks. Use it to produce a markdown report with exactly these
sections:

---

# Weekly Content Performance Report — {period}

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

## 4. Next Week Suggestions
Exactly 5 specific content ideas, each with:
- The idea itself (1–2 sentences)
- Recommended platform(s)
- The data signal that justifies it

## 5. Metrics Summary

A single markdown table with one row per platform containing the most
important numbers for that platform. Include a "n/a" for unavailable fields.

---

Content pillars to keep in mind:
{pillars}

Weekly goals:
{goals}

---

RAW DATA (JSON):
{data_json}
"""


def build_prompt(
    collected_data: dict[str, Any],
    config: dict[str, Any],
    period: str,
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

    return REPORT_TEMPLATE.format(
        period=period,
        platforms_available=platforms_available,
        pillars=pillars,
        goals=goals,
        data_json=data_json,
    )


def analyse(
    collected_data: dict[str, Any],
    config: dict[str, Any],
    period: str,
) -> str:
    """
    Send collected data to Claude and return the markdown report as a string.
    Raises on API failure (the caller decides whether to abort or skip).
    """
    api_key = config.get("anthropic_api_key", "")
    if not api_key:
        raise ValueError("anthropic_api_key is missing from config.yaml")

    client = anthropic.Anthropic(api_key=api_key)

    user_prompt = build_prompt(collected_data, config, period)

    logger.info("Sending data to Claude (%s)…", MODEL)

    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    report = message.content[0].text
    logger.info(
        "Analysis complete. Input tokens: %d, output tokens: %d",
        message.usage.input_tokens,
        message.usage.output_tokens,
    )
    return report
