"""
One-off migration: fold historical `vercel_daily` rows into the new
`web_analytics_daily` sheet with a `source` column.

Usage:
    python scripts/migrate_web_analytics.py [path/to/analytics.xlsx]

Idempotent: if `web_analytics_daily` already contains rows with
source='vercel', those are left alone; only rows missing from the new
sheet are copied in. `vercel_daily` is left in place so a rollback is
possible; remove it manually once you're happy the new sheet reflects
everything.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


DEFAULT_STORE = Path(__file__).parent.parent / "data" / "analytics.xlsx"


def migrate(store_path: Path) -> int:
    if not store_path.exists():
        print(f"No store at {store_path}; nothing to migrate.")
        return 0

    xl = pd.ExcelFile(store_path)
    if "vercel_daily" not in xl.sheet_names:
        print("No `vercel_daily` sheet — nothing to migrate.")
        return 0

    vercel_df = xl.parse("vercel_daily")
    if vercel_df.empty:
        print("`vercel_daily` sheet is empty — nothing to migrate.")
        return 0

    # Tag rows with source
    vercel_df = vercel_df.copy()
    vercel_df["source"] = "vercel"

    # Load or initialise the target sheet
    if "web_analytics_daily" in xl.sheet_names:
        target = xl.parse("web_analytics_daily")
    else:
        target = pd.DataFrame()

    # Compose merged frame — vercel rows keyed by (date, source='vercel')
    if target.empty:
        merged = vercel_df
    else:
        # Drop any existing vercel-source rows the target has, replace with source of truth
        keep_mask = ~((target.get("source") == "vercel") & target.get("date").isin(vercel_df["date"]))
        target = target[keep_mask]
        merged = pd.concat([target, vercel_df], ignore_index=True)

    # Preserve any other sheets untouched
    all_sheets: dict[str, pd.DataFrame] = {}
    for name in xl.sheet_names:
        if name == "web_analytics_daily":
            continue  # will be replaced by merged
        all_sheets[name] = xl.parse(name)
    all_sheets["web_analytics_daily"] = merged

    with pd.ExcelWriter(store_path, engine="openpyxl") as writer:
        for name, df in all_sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)

    print(f"Migrated {len(vercel_df)} rows from vercel_daily into web_analytics_daily.")
    print(f"web_analytics_daily now has {len(merged)} total rows.")
    print("`vercel_daily` left in place as a rollback safety net.")
    return len(vercel_df)


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_STORE
    migrate(path)
