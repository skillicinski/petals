"""
Main entry point for trials pipeline.

Extracts completed clinical trial data from ClinicalTrials.gov and loads to S3 Tables.
Uses SCD Type 1 (upsert) with incremental fetching based on last_update_date.

Incremental Mode:
- If table exists and LAST_RUN_TIME is set, only fetches studies updated since then
- Reduces runtime significantly for daily runs

Full Load Mode:
- If table doesn't exist or FORCE_FULL=1, fetches all COMPLETED studies
- ~311k total, filtered to ~84k INDUSTRY sponsors
"""

import faulthandler
import os
import sys

import psutil

from .extract import fetch_studies
from .load import load_studies, table_exists

faulthandler.enable(file=sys.stderr, all_threads=True)


def log(msg: str) -> None:
    """Print and flush immediately to ensure logs appear before crashes."""
    print(msg)
    sys.stdout.flush()


def log_memory(label: str) -> None:
    """Log current memory usage with a label."""
    proc = psutil.Process()
    mem = proc.memory_info()
    rss_mb = mem.rss / 1024 / 1024
    log(f"[memory] {label}: {rss_mb:.1f} MB RSS")


def run(
    table_bucket_arn: str | None = None,
    region: str = "us-east-1",
    status: str = "COMPLETED",
    sponsor_class: str | None = "INDUSTRY",
    page_size: int = 100,
    force_full: bool = False,
    last_run_time: str | None = None,
) -> dict:
    """
    Run the clinical trials pipeline.

    Args:
        table_bucket_arn: S3 Table Bucket ARN (or set TABLE_BUCKET_ARN env var)
        region: AWS region
        status: Overall status filter (default: COMPLETED)
        sponsor_class: Filter to sponsor class (default: INDUSTRY for pharma/biotech)
                       Set to None to fetch all sponsors.
        page_size: Results per API page
        force_full: Force full extraction even if table exists (or set FORCE_FULL=1)
        last_run_time: ISO timestamp from last pipeline run (or set LAST_RUN_TIME env var)

    Returns:
        Stats dict with rows_inserted and rows_updated counts
    """
    table_bucket_arn = table_bucket_arn or os.environ.get("TABLE_BUCKET_ARN")
    if not table_bucket_arn:
        raise ValueError("TABLE_BUCKET_ARN required")

    if not force_full:
        force_full = os.environ.get("FORCE_FULL", "").lower() in ("1", "true")

    last_run_time = last_run_time or os.environ.get("LAST_RUN_TIME", "").strip()

    log("[main] Clinical trials pipeline starting")
    log(f"[main] status={status}, sponsor_class={sponsor_class}")
    log(f"[main] force_full={force_full}, last_run_time={last_run_time or '(not set)'}")
    log_memory("startup")

    # Determine if incremental or full load
    updated_since = None
    if not force_full:
        log("[main] Checking for existing table...")
        if table_exists(table_bucket_arn, region=region):
            if last_run_time:
                from datetime import datetime

                updated_since = datetime.fromisoformat(last_run_time.replace("Z", "+00:00"))
                log(f"[main] Incremental mode (since last run: {updated_since.isoformat()})")
            else:
                log("[main] No last_run_time available, full extraction mode")
        else:
            log("[main] Table does not exist, full extraction mode")

    # Fetch studies
    log("[main] Fetching studies from ClinicalTrials.gov...")
    studies = list(
        fetch_studies(
            status=status,
            sponsor_class=sponsor_class,
            page_size=page_size,
            updated_since=updated_since,
        )
    )
    log(f"[main] Fetched {len(studies)} studies")
    log_memory("post-extract")

    if len(studies) == 0:
        log("[main] No new studies to load")
        return {"rows_inserted": 0, "rows_updated": 0}

    # Load to S3 Tables
    log(f"[main] Loading to S3 Tables ({table_bucket_arn})...")
    result = load_studies(
        studies,
        table_bucket_arn=table_bucket_arn,
        region=region,
    )

    log(f"[main] Done! {result['rows_inserted']} inserted, {result['rows_updated']} updated")
    log_memory("complete")
    return result


if __name__ == "__main__":
    run()
