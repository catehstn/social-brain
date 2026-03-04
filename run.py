#!/usr/bin/env python3
"""
run.py — entry point for social-brain.

Usage:
    python run.py                          # collect + analyse + save report
    python run.py --collect-only           # collect and save raw data only
    python run.py --analyse-only           # analyse most recent saved raw data
    python run.py --platform mastodon      # collect only one platform
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
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

def week_label(dt: datetime | None = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"


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
    parser.add_argument(
        "--platform",
        choices=["mastodon", "bluesky", "buttondown", "jetpack", "linkedin"],
        default=None,
        help="Collect only one platform (cannot be combined with --analyse-only).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.analyse_only and args.platform:
        logger.error("--analyse-only and --platform cannot be used together.")
        sys.exit(1)

    config = load_config()
    label = week_label()

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------
    collected: dict = {}

    if not args.analyse_only:
        from collect import collect_all

        logger.info("=== Collecting data (label: %s) ===", label)
        collected = collect_all(config, platform=args.platform)

        if not collected:
            logger.warning("No data was collected from any platform.")
        else:
            logger.info(
                "Collected data from: %s", ", ".join(collected.keys())
            )

        save_raw(collected, label)

        if args.collect_only:
            logger.info("--collect-only: done.")
            return

    # ------------------------------------------------------------------
    # Analysis
    # ------------------------------------------------------------------
    if args.analyse_only:
        collected, label = load_latest_raw()
        logger.info(
            "Loaded data from snapshot '%s' with platforms: %s",
            label,
            ", ".join(collected.keys()) if collected else "(empty)",
        )

    if not collected:
        logger.warning(
            "No collected data to analyse — the report will be minimal."
        )

    logger.info("=== Building analysis prompt ===")
    from analyse import save_prompt

    prompt_path = save_prompt(collected, config, period=label, reports_dir=REPORTS_DIR)
    logger.info("=== Done. Paste %s into claude.ai to get your report ===", prompt_path)


if __name__ == "__main__":
    main()
