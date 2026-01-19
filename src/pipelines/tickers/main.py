"""
Main entry point for ticker list pipeline.

Extracts ticker reference data from Massive API and loads to S3 Tables.
Uses SCD Type 1 (upsert) with incremental fetching based on last_updated_utc.

Incremental Mode:
- If table exists, only fetches tickers updated since last run
- Orders by last_updated_utc descending, stops when reaching older records
- Drastically reduces runtime from ~15 min to seconds for most runs

Full Load Mode:
- If table doesn't exist, fetches all tickers
"""

import faulthandler
import os
import sys

from .extract import fetch_tickers
from .load import get_incremental_cutoff, load_tickers, table_exists

# Enable faulthandler - prints traceback on segfault/SIGABRT/SIGFPE/SIGBUS
faulthandler.enable(file=sys.stderr, all_threads=True)


def log(msg: str) -> None:
    """Print and flush immediately to ensure logs appear before crashes."""
    print(msg)
    sys.stdout.flush()


def run(
    api_key: str | None = None,
    table_bucket_arn: str | None = None,
    region: str = "us-east-1",
    limit: int = 1000,
    force_full: bool = False,
    incremental_hours: int = 24,
    last_run_time: str | None = None,
) -> dict:
    """
    Run the ticker pipeline.

    Args:
        api_key: Massive API key (or set MASSIVE_API_KEY env var)
        table_bucket_arn: S3 Table Bucket ARN (or set TABLE_BUCKET_ARN env var)
        region: AWS region
        limit: Results per page when fetching
        force_full: Force full extraction even if table exists (or set FORCE_FULL=1)
        incremental_hours: Hours to look back for incremental updates (default: 24,
                           or set INCREMENTAL_HOURS env var)
        last_run_time: ISO timestamp from last pipeline run (or set LAST_RUN_TIME env var)
                       Takes precedence over incremental_hours when set.

    Returns:
        Stats dict with rows_inserted and rows_updated counts
    """
    api_key = api_key or os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        raise ValueError("MASSIVE_API_KEY required")

    table_bucket_arn = table_bucket_arn or os.environ.get("TABLE_BUCKET_ARN")
    if not table_bucket_arn:
        raise ValueError("TABLE_BUCKET_ARN required")

    if not force_full:
        force_full = os.environ.get("FORCE_FULL", "").lower() in ("1", "true")

    # Get last run time from env if not provided
    last_run_time = last_run_time or os.environ.get("LAST_RUN_TIME", "").strip()

    incremental_hours_env = os.environ.get("INCREMENTAL_HOURS")
    if incremental_hours_env:
        incremental_hours = int(incremental_hours_env)

    log("[main] Pipeline starting")
    log(f"[main] force_full={force_full}, incremental_hours={incremental_hours}")
    log(f"[main] last_run_time={last_run_time or '(not set)'}")

    # Determine if incremental or full load
    updated_since = None
    if not force_full:
        log("[main] Checking for existing table...")
        if table_exists(table_bucket_arn, region=region):
            # Use last_run_time if available (from orchestrator), otherwise fall back to hours
            if last_run_time:
                from datetime import datetime

                updated_since = datetime.fromisoformat(last_run_time.replace("Z", "+00:00"))
                log(f"[main] Incremental mode (since last run: {updated_since.isoformat()})")
            else:
                updated_since = get_incremental_cutoff(hours_ago=incremental_hours)
                cutoff = updated_since.isoformat()
                log(f"[main] Incremental mode (last {incremental_hours}h, since {cutoff})")
        else:
            log("[main] Table does not exist, full extraction mode")

    # Fetch tickers
    log("[main] Fetching tickers from Massive API...")
    tickers = list(fetch_tickers(api_key, limit=limit, updated_since=updated_since))
    log(f"[main] Fetched {len(tickers)} tickers")

    if len(tickers) == 0:
        log("[main] No new tickers to load")
        return {"rows_inserted": 0, "rows_updated": 0}

    # Load to S3 Tables
    log(f"[main] Loading to S3 Tables ({table_bucket_arn})...")
    result = load_tickers(
        tickers,
        table_bucket_arn=table_bucket_arn,
        region=region,
    )

    log(f"[main] Done! {result['rows_inserted']} inserted, {result['rows_updated']} updated")
    return result


if __name__ == "__main__":
    run()
