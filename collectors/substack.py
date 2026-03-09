from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from collectors._helpers import _utcnow, _iso

logger = logging.getLogger(__name__)


# Substack email analytics export columns (case-insensitive)
def collect_substack(substack_drops_dir: str | Path = "substack_drops") -> dict[str, Any] | None:
    """
    Read the most recently modified Substack email analytics CSV export
    from the substack_drops/ directory.

    Handles both the current export format (title, post_date, delivered,
    open_rate, likes, comments, shares) and the older format (Subject, Date,
    Recipients, Opens, Open rate, Clicks, Unsubscribes).
    """
    drops_path = Path(substack_drops_dir)
    csv_files = sorted(
        drops_path.glob("*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not csv_files:
        logger.info("Substack: no CSV files found in %s — skipping", drops_path)
        return None

    csv_path = csv_files[0]
    file_age = _utcnow() - datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)
    if file_age > timedelta(weeks=2):
        logger.warning(
            "Substack: export file %s is %d days old — data may be missing recent activity. "
            "Consider downloading a fresh export.",
            csv_path.name,
            file_age.days,
        )
    logger.info("Substack: reading %s", csv_path)

    try:
        df = pd.read_csv(csv_path)
        df.columns = [c.strip().lower() for c in df.columns]

        # Detect format by presence of key columns
        if "title" in df.columns:
            # Current Substack export format
            rename = {
                "title": "subject",
                "post_date": "date",
                "delivered": "recipients",
                "open_rate": "open_rate",
                "opens": "opens",
                "likes": "likes",
                "comments": "comments",
                "shares": "shares",
                "signups_within_1_day": "new_signups",
                "subscribes": "new_subscribers",
            }
        else:
            # Older export format
            rename = {
                "subject": "subject",
                "date": "date",
                "recipients": "recipients",
                "opens": "opens",
                "open rate": "open_rate",
                "clicks": "clicks",
                "click rate": "click_rate",
                "unsubscribes": "unsubscribes",
            }

        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        keep = [c for c in rename.values() if c in df.columns]
        df = df[keep].copy()
        df = df.dropna(how="all")

        # Normalise date to YYYY-MM-DD
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

        # Coerce integer columns
        int_cols = ["recipients", "opens", "clicks", "likes", "comments",
                    "shares", "new_signups", "new_subscribers", "unsubscribes"]
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        # open_rate / click_rate: normalise to 0–1 if stored as whole percent
        for col in ["open_rate", "click_rate"]:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .str.replace("%", "", regex=False)
                    .str.strip()
                    .pipe(pd.to_numeric, errors="coerce")
                )
                if df[col].dropna().gt(1).any():
                    df[col] = (df[col] / 100).round(4)

        emails = df.to_dict(orient="records")

        summary = {
            "total_recipients": int(df["recipients"].sum()) if "recipients" in df.columns else None,
            "total_opens": int(df["opens"].sum()) if "opens" in df.columns else None,
            "total_clicks": int(df["clicks"].sum()) if "clicks" in df.columns else None,
            "total_unsubscribes": int(df["unsubscribes"].sum()) if "unsubscribes" in df.columns else None,
            "avg_open_rate": round(float(df["open_rate"].mean()), 4) if "open_rate" in df.columns else None,
            "avg_click_rate": round(float(df["click_rate"].mean()), 4) if "click_rate" in df.columns else None,
        }

        logger.info("Substack: parsed %d emails from %s", len(emails), csv_path.name)
        return {
            "platform": "substack",
            "source_file": csv_path.name,
            "collected_at": _iso(_utcnow()),
            "summary": summary,
            "emails": emails,
        }

    except Exception as exc:
        logger.error("Substack CSV parsing failed (%s): %s", csv_path, exc)
        return None
