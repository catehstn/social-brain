#!/usr/bin/env python3
"""
run.py — entry point for social-brain.

Usage:
    python run.py                          # collect + build prompt (default: 2 weeks)
    python run.py --months 3               # collect 3 months of history
    python run.py --collect-only           # collect and save raw data only
    python run.py --analyse-only           # build prompt from most recent saved data
    python run.py --platform <name>        # collect only one platform
    python run.py --update                 # collect + build a compact update prompt for the same chat
    python run.py --analyse-only --update  # update prompt from most recent saved data
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "config.yaml"
DATA_DIR = ROOT / "data" / "weekly"
REPORTS_DIR = ROOT / "reports"

REQUIRED_CONFIG_KEYS = [
    "mastodon_instance",
    "mastodon_handle",
    "bluesky_handle",
    "buttondown_api_key",
    "jetpack_site",
    "jetpack_access_token",
]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        logger.error("config.yaml not found at %s", CONFIG_PATH)
        logger.error(
            "Create it from the template:  cp config.example.yaml config.yaml"
        )
        logger.error("Then fill in your API keys before running social-brain.")
        sys.exit(1)

    with CONFIG_PATH.open() as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        logger.error("config.yaml is empty or not valid YAML.")
        sys.exit(1)

    missing = [k for k in REQUIRED_CONFIG_KEYS if not config.get(k)]
    if missing:
        logger.warning(
            "The following config keys are missing or empty: %s",
            ", ".join(missing),
        )
        logger.warning(
            "Collectors that need these keys will be skipped or may fail."
        )

    return config


# ---------------------------------------------------------------------------
# Period label  →  e.g.  2025-W22
# ---------------------------------------------------------------------------

def week_label(dt: datetime | None = None, months: int | None = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    year, week, _ = dt.isocalendar()
    label = f"{year}-W{week:02d}"
    if months:
        label += f"-{months}m"
    return label


# ---------------------------------------------------------------------------
# File I/O helpers
# ---------------------------------------------------------------------------

def save_raw(data: dict, label: str) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{label}.json"
    with path.open("w") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info("Raw data saved → %s", path)
    return path


def load_latest_raw() -> tuple[dict, str]:
    """Return (data, label) for the most recently modified JSON snapshot."""
    snapshots = sorted(DATA_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not snapshots:
        logger.error("No raw data snapshots found in %s", DATA_DIR)
        logger.error("Run without --analyse-only first to collect data.")
        sys.exit(1)
    path = snapshots[0]
    label = path.stem
    logger.info("Loading raw data from %s", path)
    with path.open() as f:
        return json.load(f), label


def _platform_summary(name: str, data: dict) -> str:
    """Return a brief one-line description of what was collected for a platform."""
    try:
        if name == "mastodon":
            followers = data.get("account", {}).get("followers", "?")
            posts = len(data.get("posts", []))
            return f"{followers} followers, {posts} posts"
        if name == "bluesky":
            posts = len(data.get("posts", []))
            return f"{posts} posts"
        if name == "buttondown":
            newsletters = len(data.get("newsletters", []))
            return f"{newsletters} newsletter(s)"
        if name == "jetpack":
            return f"{data.get('total_views', '?')} views"
        if name == "linkedin":
            days = len(data.get("daily_engagement", []))
            posts = len(data.get("top_posts_by_engagement", []))
            return f"{days} days engagement, {posts} top posts"
        if name == "substack":
            return f"{len(data.get('emails', []))} emails"
        if name == "vercel":
            return f"{data.get('page_views', '?')} views"
        if name == "amazon":
            markets = len(data.get("by_marketplace", {}))
            return f"{markets} marketplace(s)"
        if name == "goatcounter":
            return f"{data.get('total_pageviews', '?')} pageviews"
        if name == "oreilly":
            count = data.get("payment_count", 0)
            total = data.get("total_paid", 0.0)
            currencies = data.get("currencies") or ["?"]
            return f"{count} payment(s), {currencies[0]} {total:.2f} total"
        if name == "calendly":
            bookings = data.get("total_bookings", 0)
            lead = data.get("lead_gen_bookings")
            if lead is not None:
                return f"{bookings} booking(s) ({lead} lead gen)"
            return f"{bookings} booking(s)"
        if name == "mentions":
            sources = data.get("sources", {})
            hn = len(sources.get("hackernews", []))
            mastodon = len(sources.get("mastodon", []))
            bluesky = len(sources.get("bluesky", []))
            return f"HN: {hn}, Mastodon: {mastodon}, Bluesky: {bluesky}"
        if name == "upcoming":
            sources = data.get("sources", {})
            total = sum(len(v) for v in sources.values() if isinstance(v, list))
            return f"{total} scheduled item(s)"
    except Exception:
        pass
    return "ok"


def _print_run_summary(
    collected: dict,
    all_platforms: list[str],
    prompt_path: Path | None = None,
    update: bool = False,
) -> None:
    """Print a clean end-of-run summary: what was collected and what wasn't."""
    print("\n" + "=" * 52)
    print("  Run complete")
    print("=" * 52)

    present = [p for p in all_platforms if p in collected]
    missing = [p for p in all_platforms if p not in collected]

    if present:
        print("\nCollected:")
        for p in present:
            print(f"  ✓  {p:<14} {_platform_summary(p, collected[p])}")

    if missing:
        print("\nNot collected (no config, no files, or no data):")
        for p in missing:
            print(f"  –  {p}")

    if prompt_path:
        action = "Paste into your existing claude.ai chat" if update else "Paste into claude.ai"
        size_kb = f"{prompt_path.stat().st_size // 1024} KB" if prompt_path.exists() else ""
        size_str = f" ({size_kb})" if size_kb else ""
        print(f"\nPrompt{size_str}: {prompt_path}")
        print(f"→  {action}")

    print("=" * 52 + "\n")


def since_last_run() -> datetime | None:
    """
    Return the mtime of the most recent snapshot if it's older than 2 weeks,
    so the caller can extend the lookback window to cover the gap.
    Returns None if the last run was within the default window.
    """
    snapshots = sorted(DATA_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not snapshots:
        return None
    last_mtime = datetime.fromtimestamp(snapshots[0].stat().st_mtime, tz=timezone.utc)
    if last_mtime < datetime.now(timezone.utc) - timedelta(weeks=2):
        return last_mtime
    return None



# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="social-brain: collect social analytics and generate a weekly report."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--collect-only",
        action="store_true",
        help="Collect data and save raw JSON, but skip analysis.",
    )
    mode.add_argument(
        "--analyse-only",
        action="store_true",
        help="Skip collection and analyse the most recent saved raw data.",
    )
    from collectors import PLATFORM_COLLECTORS
    parser.add_argument(
        "--platform",
        choices=sorted(PLATFORM_COLLECTORS.keys()),
        default=None,
        help="Collect only one platform (cannot be combined with --analyse-only).",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=None,
        metavar="N",
        help="Collect N months of history instead of the default 2-week window.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Generate a compact follow-up prompt (prompt-YYYY-WNN-update.txt) for use in the same claude.ai chat as the original report.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.analyse_only and args.platform:
        logger.error("--analyse-only and --platform cannot be used together.")
        sys.exit(1)

    config = load_config()

    since: datetime | None = None
    if args.months:
        since = datetime.now(timezone.utc) - timedelta(days=args.months * 30)
        logger.info("Lookback: %d month(s) (since %s)", args.months, since.date())
    elif not args.analyse_only:
        gap = since_last_run()
        if gap:
            since = gap
            logger.info("Last run was %s — extending lookback to cover gap", gap.date())

    label = week_label(months=args.months)

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------
    collected: dict = {}

    if not args.analyse_only:
        from collect import collect_all

        logger.info("=== Collecting data (label: %s) ===", label)
        collected = collect_all(config, platform=args.platform, since=since)

        if not collected:
            logger.warning("No data was collected from any platform.")
        else:
            logger.info(
                "Collected data from: %s", ", ".join(collected.keys())
            )

        save_raw(collected, label)

        # ------------------------------------------------------------------
        # Persistent store — upsert into analytics.xlsx
        # ------------------------------------------------------------------
        if collected and not args.platform:
            from store import update as store_update, get_known_platforms, STORE_PATH

            known = get_known_platforms()
            new_platforms = set(collected.keys()) - known - {"upcoming", "mentions"}

            if new_platforms and since is None:
                # First time seeing these platforms — backfill 3 months
                logger.info(
                    "Store: new platform(s) detected (%s) — backfilling 3 months",
                    ", ".join(sorted(new_platforms)),
                )
                backfill_since = datetime.now(timezone.utc) - timedelta(days=90)
                backfill = collect_all(
                    config,
                    platform=None,
                    since=backfill_since,
                )
                store_update(backfill)
            else:
                store_update(collected)

        if args.collect_only:
            from collectors import PLATFORM_COLLECTORS
            _print_run_summary(collected, list(PLATFORM_COLLECTORS.keys()))
            return

    # ------------------------------------------------------------------
    # Analysis
    # ------------------------------------------------------------------
    if args.analyse_only:
        collected, label = load_latest_raw()

    if not collected:
        logger.warning(
            "No collected data to analyse — the report will be minimal."
        )

    logger.info("=== Building analysis prompt ===")
    from analyse import save_prompt
    from collectors import PLATFORM_COLLECTORS

    prompt_path = save_prompt(collected, config, period=label, reports_dir=REPORTS_DIR, months=args.months, update=args.update)
    _print_run_summary(collected, list(PLATFORM_COLLECTORS.keys()), prompt_path=prompt_path, update=args.update)


if __name__ == "__main__":
    main()
