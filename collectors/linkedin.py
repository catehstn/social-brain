from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from collectors._helpers import _utcnow, _iso

logger = logging.getLogger(__name__)


_LINKEDIN_COLUMN_MAP = {
    "post title": "title",
    "content": "title",
    "post content": "title",
    "date": "date",
    "published date": "date",
    "impressions": "impressions",
    "clicks": "clicks",
    "reactions": "reactions",
    "comments": "comments",
    "shares": "shares",
    "reposts": "shares",
    "ctr (clicks / impressions)": "ctr",
    "engagement rate": "engagement_rate",
}


def _fetch_linkedin_post_text(url: str) -> str | None:
    """Fetch the post text from a public LinkedIn post URL via og:description."""
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        }
        r = httpx.get(url, headers=headers, follow_redirects=True, timeout=15)
        r.raise_for_status()
        m = re.search(
            r'<meta[^>]+property="og:description"[^>]+content="([^"]+)"',
            r.text,
        )
        if m:
            # Strip trailing comment count appended by LinkedIn e.g. " | 28 comments on LinkedIn"
            text = re.sub(r"\s*\|\s*\d+ comments? on LinkedIn$", "", m.group(1)).strip()
            return text
    except Exception as exc:
        logger.debug("LinkedIn post fetch failed (%s): %s", url, exc)
    return None


def _parse_linkedin_xlsx(path: Path) -> dict[str, Any]:
    """Parse the multi-sheet LinkedIn analytics XLSX export."""
    xl = pd.ExcelFile(path)
    sheets = {s.upper(): s for s in xl.sheet_names}
    result: dict[str, Any] = {}

    # DISCOVERY — overall impressions / members reached
    if "DISCOVERY" in sheets:
        df = xl.parse(sheets["DISCOVERY"])
        discovery: dict[str, Any] = {}
        for _, row in df.iterrows():
            key = str(row.iloc[0]).strip().lower()
            val = row.iloc[1]
            if "impression" in key:
                discovery["total_impressions"] = int(val) if pd.notna(val) else None
            elif "member" in key:
                discovery["members_reached"] = int(val) if pd.notna(val) else None
        result["discovery"] = discovery

    # ENGAGEMENT — daily impressions + engagements
    if "ENGAGEMENT" in sheets:
        df = xl.parse(sheets["ENGAGEMENT"])
        df.columns = [str(c).strip().lower() for c in df.columns]
        df = df.dropna(how="all")
        daily = []
        for _, row in df.iterrows():
            entry: dict[str, Any] = {}
            for col in df.columns:
                val = row[col]
                if pd.notna(val):
                    entry[col] = str(val) if "date" in col else (int(val) if isinstance(val, float) else val)
            if entry:
                daily.append(entry)
        result["daily_engagement"] = daily

    # TOP POSTS — two side-by-side tables (by engagement, by impressions)
    if "TOP POSTS" in sheets:
        df = xl.parse(sheets["TOP POSTS"], header=None)
        # Find the header row (contains "Post URL")
        header_row = None
        for i, row in df.iterrows():
            if row.astype(str).str.contains("Post URL", case=False).any():
                header_row = i
                break

        top_by_engagement: list[dict] = []
        top_by_impressions: list[dict] = []

        if header_row is not None:
            data = df.iloc[header_row + 1:].reset_index(drop=True)
            # Left table: cols 0–2 (URL, date, engagements)
            for _, row in data.iterrows():
                url, date, eng = row.iloc[0], row.iloc[1], row.iloc[2]
                if pd.notna(url) and str(url).startswith("http"):
                    top_by_engagement.append({
                        "url": str(url),
                        "date": str(date),
                        "engagements": int(eng) if pd.notna(eng) else None,
                    })
            # Right table: cols 4–6 (URL, date, impressions)
            if len(df.columns) >= 7:
                for _, row in data.iterrows():
                    url, date, imp = row.iloc[4], row.iloc[5], row.iloc[6]
                    if pd.notna(url) and str(url).startswith("http"):
                        top_by_impressions.append({
                            "url": str(url),
                            "date": str(date),
                            "impressions": int(imp) if pd.notna(imp) else None,
                        })

        # Fetch post text for all unique URLs
        all_urls = list({
            p["url"]
            for p in top_by_engagement + top_by_impressions
        })
        post_texts: dict[str, str | None] = {}
        for i, post_url in enumerate(all_urls):
            if i > 0:
                time.sleep(1)
            text = _fetch_linkedin_post_text(post_url)
            post_texts[post_url] = text
            logger.debug("LinkedIn post text fetched: %s chars", len(text) if text else 0)

        for p in top_by_engagement:
            p["text"] = post_texts.get(p["url"])
        for p in top_by_impressions:
            p["text"] = post_texts.get(p["url"])

        result["top_posts_by_engagement"] = top_by_engagement
        result["top_posts_by_impressions"] = top_by_impressions

    # FOLLOWERS — total + daily new
    if "FOLLOWERS" in sheets:
        df = xl.parse(sheets["FOLLOWERS"], header=None)
        followers: dict[str, Any] = {}
        # First row header contains total followers
        first_col = str(df.iloc[0, 0]) if pd.notna(df.iloc[0, 0]) else ""
        if "follower" in first_col.lower():
            followers["total_followers"] = int(df.iloc[0, 1]) if pd.notna(df.iloc[0, 1]) else None
        # Find "Date" / "New followers" header row
        daily_followers = []
        for i, row in df.iterrows():
            if str(row.iloc[0]).strip().lower() == "date":
                for _, drow in df.iloc[i + 1:].iterrows():
                    if pd.notna(drow.iloc[0]) and pd.notna(drow.iloc[1]):
                        daily_followers.append({
                            "date": str(drow.iloc[0]),
                            "new_followers": int(drow.iloc[1]),
                        })
                break
        followers["daily_new_followers"] = daily_followers
        result["followers"] = followers

    # DEMOGRAPHICS — top job titles / industries
    if "DEMOGRAPHICS" in sheets:
        df = xl.parse(sheets["DEMOGRAPHICS"])
        df.columns = [str(c).strip().lower() for c in df.columns]
        df = df.dropna(how="all")
        result["demographics"] = df.to_dict(orient="records")

    return result


def _parse_linkedin_csv(path: Path) -> dict[str, Any]:
    """Parse a per-post LinkedIn analytics CSV export."""
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    rename = {k: v for k, v in _LINKEDIN_COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)
    keep = list(dict.fromkeys(c for c in _LINKEDIN_COLUMN_MAP.values() if c in df.columns))
    df = df[keep].copy()
    metric_cols = [c for c in ["impressions", "clicks", "reactions", "comments", "shares"] if c in df.columns]
    df = df.dropna(subset=metric_cols, how="all")
    for col in metric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    posts = df.to_dict(orient="records")
    return {
        "posts": posts,
        "summary": {
            "total_impressions": int(df["impressions"].sum()) if "impressions" in df.columns else None,
            "total_clicks": int(df["clicks"].sum()) if "clicks" in df.columns else None,
            "total_reactions": int(df["reactions"].sum()) if "reactions" in df.columns else None,
            "total_comments": int(df["comments"].sum()) if "comments" in df.columns else None,
            "total_shares": int(df["shares"].sum()) if "shares" in df.columns else None,
        },
    }


def collect_linkedin(linkedin_drops_dir: str | Path = "linkedin_drops") -> dict[str, Any] | None:
    """
    Read the most recently modified LinkedIn analytics export (CSV or XLSX)
    from the linkedin_drops/ directory.
    """
    drops_path = Path(linkedin_drops_dir)
    export_files = sorted(
        list(drops_path.glob("*.csv")) + list(drops_path.glob("*.xlsx")),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not export_files:
        logger.info("LinkedIn: no export files found in %s — skipping", drops_path)
        return None

    export_path = export_files[0]
    file_age = _utcnow() - datetime.fromtimestamp(export_path.stat().st_mtime, tz=timezone.utc)
    if file_age > timedelta(weeks=2):
        logger.warning(
            "LinkedIn: export file %s is %d days old — data may be missing recent activity. "
            "Consider downloading a fresh export.",
            export_path.name,
            file_age.days,
        )
    logger.info("LinkedIn: reading %s", export_path)

    try:
        if export_path.suffix.lower() == ".xlsx":
            data = _parse_linkedin_xlsx(export_path)
        else:
            data = _parse_linkedin_csv(export_path)

        post_count = (
            len(data.get("top_posts_by_engagement", []))
            or len(data.get("posts", []))
        )
        logger.info("LinkedIn: parsed data from %s (%d top posts)", export_path.name, post_count)
        return {
            "platform": "linkedin",
            "source_file": export_path.name,
            "collected_at": _iso(_utcnow()),
            **data,
        }

    except Exception as exc:
        logger.error("LinkedIn parsing failed (%s): %s", export_path, exc)
        return None
