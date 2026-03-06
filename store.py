"""
store.py — persistent analytics store.

Maintains data/analytics.xlsx as a running history of all collected data.
Each platform's data lives in one or more sheets and is upserted on each run —
metrics that change (likes, boosts, rank) get overwritten; new rows are appended.

On first run for a platform (no rows yet), the caller should collect 3 months
of data so history is populated from day one. Subsequent runs can use any
window — rows accumulate and update in place.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

STORE_PATH = Path(__file__).parent / "data" / "analytics.xlsx"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _load(store_path: Path, sheet: str) -> pd.DataFrame:
    """Load a sheet, returning an empty DataFrame if file/sheet doesn't exist."""
    if not store_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(store_path, sheet_name=sheet, dtype=str)
    except Exception:
        return pd.DataFrame()


def _upsert(existing: pd.DataFrame, new: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """
    Merge new rows into existing by key columns.
    Existing rows whose keys appear in new are replaced; others are kept.
    """
    if new.empty:
        return existing
    if existing.empty:
        return new.copy()

    # Drop existing rows that will be replaced
    try:
        idx_existing = existing.set_index(keys).index
        idx_new = new.set_index(keys).index
        mask = idx_existing.isin(idx_new)
        existing = existing[~mask]
    except KeyError:
        pass  # key columns not yet in existing sheet — safe to concat

    if existing.empty:
        return new.copy()
    return pd.concat([existing, new], ignore_index=True)


def get_known_platforms(store_path: Path = STORE_PATH) -> set[str]:
    """Return platforms that already have rows in the store."""
    if not store_path.exists():
        return set()
    try:
        xl = pd.ExcelFile(store_path)
        known = set()
        # Map sheet prefix → platform name
        prefix_map = {
            "mastodon": "mastodon",
            "bluesky": "bluesky",
            "jetpack": "jetpack",
            "linkedin": "linkedin",
            "buttondown": "buttondown",
            "vercel": "vercel",
            "amazon": "amazon",
            "mentions": "mentions",
        }
        for sheet in xl.sheet_names:
            for prefix, platform in prefix_map.items():
                if sheet.startswith(prefix):
                    df = xl.parse(sheet)
                    if not df.empty:
                        known.add(platform)
        return known
    except Exception:
        return set()


# ---------------------------------------------------------------------------
# Per-platform upsert logic
# ---------------------------------------------------------------------------

def _process_mastodon(collected: dict, sheets: dict, store_path: Path, now: str) -> None:
    posts = collected.get("posts", [])
    if posts:
        rows = [{
            "post_id": p.get("id", ""),
            "handle": collected.get("handle", ""),
            "created_at": p.get("created_at", ""),
            "content": str(p.get("content", ""))[:500],
            "favourites": p.get("favourites", 0),
            "boosts": p.get("boosts", 0),
            "replies": p.get("replies", 0),
            "has_attachment": p.get("has_attachment", False),
            "last_updated": now,
        } for p in posts if p.get("id")]
        if rows:
            df_new = pd.DataFrame(rows)
            sheets["mastodon_posts"] = _upsert(_load(store_path, "mastodon_posts"), df_new, ["post_id"])

    acct = collected.get("account", {})
    if acct:
        df_new = pd.DataFrame([{
            "date": now[:10],
            "platform": "mastodon",
            "metric": "followers",
            "value": acct.get("followers", 0),
        }])
        sheets["account_snapshots"] = _upsert(
            sheets.get("account_snapshots", _load(store_path, "account_snapshots")),
            df_new, ["date", "platform", "metric"],
        )

    for f in collected.get("new_follows", []):
        rows = [{
            "platform": "mastodon",
            "followed_at": f.get("followed_at", ""),
            "account": f.get("account", ""),
            "display_name": f.get("display_name", ""),
            "followers": f.get("followers", 0),
        }]
        df_new = pd.DataFrame(rows)
        sheets["follows"] = _upsert(
            sheets.get("follows", _load(store_path, "follows")),
            df_new, ["platform", "followed_at", "account"],
        )


def _process_bluesky(collected: dict, sheets: dict, store_path: Path, now: str) -> None:
    posts = collected.get("posts", [])
    if posts:
        rows = [{
            "post_uri": p.get("uri", ""),
            "handle": collected.get("handle", ""),
            "created_at": p.get("created_at", ""),
            "text": str(p.get("text", ""))[:500],
            "likes": p.get("likes", 0),
            "reposts": p.get("reposts", 0),
            "replies": p.get("replies", 0),
            "has_attachment": p.get("has_attachment", False),
            "last_updated": now,
        } for p in posts if p.get("uri")]
        if rows:
            df_new = pd.DataFrame(rows)
            sheets["bluesky_posts"] = _upsert(_load(store_path, "bluesky_posts"), df_new, ["post_uri"])

    for f in collected.get("new_follows", []):
        rows = [{
            "platform": "bluesky",
            "followed_at": f.get("followed_at", ""),
            "account": f.get("handle", ""),
            "display_name": f.get("display_name", ""),
            "followers": f.get("followers", 0),
        }]
        df_new = pd.DataFrame(rows)
        sheets["follows"] = _upsert(
            sheets.get("follows", _load(store_path, "follows")),
            df_new, ["platform", "followed_at", "account"],
        )


def _process_jetpack(collected: dict, sheets: dict, store_path: Path, now: str) -> None:
    daily = collected.get("daily_views", [])
    if daily:
        df_new = pd.DataFrame([{"date": d["date"], "views": d["views"]} for d in daily if d.get("date")])
        sheets["jetpack_daily"] = _upsert(_load(store_path, "jetpack_daily"), df_new, ["date"])

    top_posts = collected.get("top_posts", [])
    if top_posts:
        df_new = pd.DataFrame([{
            "url": p.get("href", ""),
            "title": p.get("title", ""),
            "views": p.get("views", 0),
            "last_updated": now,
        } for p in top_posts if p.get("href")])
        sheets["jetpack_top_posts"] = _upsert(_load(store_path, "jetpack_top_posts"), df_new, ["url"])

    referrers = collected.get("referrers", [])
    if referrers:
        df_new = pd.DataFrame([{
            "name": r.get("name", ""),
            "views": r.get("views", 0),
            "last_updated": now,
        } for r in referrers if r.get("name")])
        sheets["jetpack_referrers"] = _upsert(_load(store_path, "jetpack_referrers"), df_new, ["name"])


def _process_linkedin(collected: dict, sheets: dict, store_path: Path, now: str) -> None:
    daily = collected.get("daily_engagement", [])
    if daily:
        df_new = pd.DataFrame([{
            "date": d.get("date", ""),
            "impressions": d.get("impressions", 0),
            "engagements": d.get("engagements", 0),
            "new_followers": d.get("new_followers", 0),
        } for d in daily if d.get("date")])
        sheets["linkedin_daily"] = _upsert(_load(store_path, "linkedin_daily"), df_new, ["date"])

    # Merge engagement and impressions lists by URL
    all_posts: dict[str, dict] = {}
    for p in collected.get("top_posts_by_engagement", []):
        url = p.get("url", "")
        if url:
            all_posts[url] = {
                "url": url,
                "date": p.get("date", ""),
                "engagements": p.get("engagements"),
                "impressions": None,
                "text": str(p.get("text") or "")[:500],
                "last_updated": now,
            }
    for p in collected.get("top_posts_by_impressions", []):
        url = p.get("url", "")
        if url:
            if url in all_posts:
                all_posts[url]["impressions"] = p.get("impressions")
            else:
                all_posts[url] = {
                    "url": url,
                    "date": p.get("date", ""),
                    "engagements": None,
                    "impressions": p.get("impressions"),
                    "text": str(p.get("text") or "")[:500],
                    "last_updated": now,
                }
    if all_posts:
        df_new = pd.DataFrame(list(all_posts.values()))
        sheets["linkedin_posts"] = _upsert(_load(store_path, "linkedin_posts"), df_new, ["url"])


def _process_buttondown(collected: dict, sheets: dict, store_path: Path, now: str) -> None:
    emails = collected.get("newsletters", [])
    if emails:
        df_new = pd.DataFrame([{
            "email_id": e.get("id", ""),
            "newsletter": e.get("newsletter", ""),
            "subject": e.get("subject", ""),
            "send_date": e.get("send_date", ""),
            "recipients": e.get("recipients", 0),
            "opens": e.get("opens", 0),
            "clicks": e.get("clicks", 0),
            "open_rate": e.get("open_rate"),
            "click_rate": e.get("click_rate"),
            "unsubscribes": e.get("unsubscribes", 0),
            "new_subscribers": e.get("new_subscribers", 0),
            "last_updated": now,
        } for e in emails if e.get("id")])
        sheets["buttondown_emails"] = _upsert(_load(store_path, "buttondown_emails"), df_new, ["email_id"])

    subscriber_counts = collected.get("subscriber_counts", {})
    if subscriber_counts:
        df_new = pd.DataFrame([{
            "date": now[:10],
            "newsletter": name,
            "subscribers": count,
        } for name, count in subscriber_counts.items()])
        sheets["buttondown_subscribers"] = _upsert(
            _load(store_path, "buttondown_subscribers"), df_new, ["date", "newsletter"]
        )


def _process_vercel(collected: dict, sheets: dict, store_path: Path, now: str) -> None:
    daily = collected.get("daily", [])
    if daily:
        df_new = pd.DataFrame([{
            "date": d.get("date", ""),
            "page_views": d.get("page_views", 0),
            "visitors": d.get("visitors", 0),
        } for d in daily if d.get("date")])
        sheets["vercel_daily"] = _upsert(_load(store_path, "vercel_daily"), df_new, ["date"])


def _process_amazon(collected: dict, sheets: dict, store_path: Path, now: str) -> None:
    rows = []
    for marketplace, books in collected.get("by_marketplace", {}).items():
        for b in books:
            rows.append({
                "asin": b.get("asin", ""),
                "marketplace": marketplace,
                "title": b.get("title", ""),
                "rank": b.get("best_sellers_rank"),
                "rating": b.get("rating"),
                "reviews": b.get("reviews"),
                "last_updated": now,
            })
    if rows:
        df_new = pd.DataFrame(rows)
        sheets["amazon"] = _upsert(_load(store_path, "amazon"), df_new, ["asin", "marketplace"])


def _process_mentions(collected: dict, sheets: dict, store_path: Path, now: str) -> None:
    sources = collected.get("sources", {})

    hn_hits = sources.get("hacker_news", [])
    if hn_hits:
        df_new = pd.DataFrame([{
            "object_id": h.get("objectID", ""),
            "type": h.get("type", ""),
            "title": h.get("title", ""),
            "url": h.get("url", ""),
            "points": h.get("points", 0),
            "num_comments": h.get("num_comments", 0),
            "created_at": h.get("created_at", ""),
            "domain": h.get("domain", ""),
        } for h in hn_hits if h.get("objectID")])
        sheets["hn_mentions"] = _upsert(_load(store_path, "hn_mentions"), df_new, ["object_id"])

    masto_mentions = sources.get("mastodon", [])
    if masto_mentions:
        df_new = pd.DataFrame([{
            "notification_id": m.get("id", ""),
            "account": m.get("account", {}).get("acct", "") if isinstance(m.get("account"), dict) else "",
            "content": str(m.get("status", {}).get("content", "") if isinstance(m.get("status"), dict) else "")[:300],
            "created_at": m.get("created_at", ""),
        } for m in masto_mentions if m.get("id")])
        sheets["mastodon_mentions"] = _upsert(
            _load(store_path, "mastodon_mentions"), df_new, ["notification_id"]
        )

    bsky_mentions = sources.get("bluesky", [])
    if bsky_mentions:
        df_new = pd.DataFrame([{
            "uri": m.get("uri", m.get("cid", "")),
            "author": m.get("author", {}).get("handle", "") if isinstance(m.get("author"), dict) else "",
            "text": str(m.get("record", {}).get("text", "") if isinstance(m.get("record"), dict) else "")[:300],
            "indexed_at": m.get("indexedAt", ""),
        } for m in bsky_mentions])
        if not df_new.empty and "uri" in df_new.columns:
            sheets["bluesky_mentions"] = _upsert(
                _load(store_path, "bluesky_mentions"), df_new, ["uri"]
            )

    gsc_rows = sources.get("google_search_console", [])
    if gsc_rows:
        df_new = pd.DataFrame([{
            "site": g.get("site", ""),
            "query": g.get("query", ""),
            "page": g.get("page", ""),
            "clicks": g.get("clicks", 0),
            "impressions": g.get("impressions", 0),
            "ctr": g.get("ctr"),
            "position": g.get("position"),
            "last_updated": now,
        } for g in gsc_rows])
        sheets["gsc_queries"] = _upsert(
            _load(store_path, "gsc_queries"), df_new, ["site", "query", "page"]
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_PROCESSORS = {
    "mastodon": _process_mastodon,
    "bluesky": _process_bluesky,
    "jetpack": _process_jetpack,
    "linkedin": _process_linkedin,
    "buttondown": _process_buttondown,
    "vercel": _process_vercel,
    "amazon": _process_amazon,
    "mentions": _process_mentions,
}


def update(collected: dict[str, Any], store_path: Path = STORE_PATH) -> None:
    """
    Upsert all collected platform data into the persistent Excel store.
    Sheets not touched this run are preserved as-is.
    """
    store_path.parent.mkdir(parents=True, exist_ok=True)
    now = _now_str()
    sheets: dict[str, pd.DataFrame] = {}

    for platform, processor in _PROCESSORS.items():
        if platform in collected:
            try:
                processor(collected[platform], sheets, store_path, now)
            except Exception as exc:
                logger.error("Store: failed to process %s: %s", platform, exc)

    if not sheets:
        logger.info("Store: nothing to write")
        return

    # Preserve sheets we didn't touch
    existing_sheets: dict[str, pd.DataFrame] = {}
    if store_path.exists():
        try:
            xl = pd.ExcelFile(store_path)
            for name in xl.sheet_names:
                if name not in sheets:
                    existing_sheets[name] = xl.parse(name)
        except Exception as exc:
            logger.warning("Store: could not read existing sheets: %s", exc)

    all_sheets = {**existing_sheets, **sheets}

    with pd.ExcelWriter(store_path, engine="openpyxl") as writer:
        for name, df in all_sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)

    total_rows = sum(len(df) for df in sheets.values())
    logger.info(
        "Store: updated %d sheet(s), %d rows upserted → %s",
        len(sheets), total_rows, store_path,
    )
