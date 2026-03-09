from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _default_since() -> datetime:
    """Default lookback: 2 weeks."""
    return _utcnow() - timedelta(weeks=2)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _strip_html(html: str) -> str:
    """Remove HTML tags and decode common entities."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&#8217;", "'", text)
    text = re.sub(r"&#8220;|&#8221;", '"', text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def _strip_html_simple(html: str) -> str:
    """Minimal HTML stripper for mention content."""
    return re.sub(r"<[^>]+>", " ", html).strip()
