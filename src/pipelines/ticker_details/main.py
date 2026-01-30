"""
Main entry point for ticker details enrichment pipeline.

Reads tickers from reference.tickers table, fetches detailed information
from Massive API, and loads to reference.ticker_details table.

Filters to pharma/biotech/medical tickers to reduce API calls and focus
on tickers relevant for clinical trial sponsor matching.

Incremental Mode:
- Only fetches details for tickers updated since last run
- Uses last_updated_utc from tickers table to determine freshness

Rate Limiting:
- Respects Massive free tier limit (5 calls/min)
- Initial backfill for ~3,600 pharma-like tickers takes ~12 hours
- Incremental runs typically complete in minutes
"""

import faulthandler
import os
import sys
from datetime import datetime

import polars as pl
import psutil
from pyiceberg.catalog import load_catalog

from .extract import fetch_ticker_details_batch, is_pharma_like
from .load import load_ticker_details, table_exists

# Enable faulthandler - prints traceback on segfault
faulthandler.enable(file=sys.stderr, all_threads=True)


def log(msg: str) -> None:
    """Print and flush immediately."""
    print(msg)
    sys.stdout.flush()


def log_memory(label: str) -> None:
    """Log current memory usage with a label."""
    proc = psutil.Process()
    mem = proc.memory_info()
    rss_mb = mem.rss / 1024 / 1024
    log(f"[memory] {label}: {rss_mb:.1f} MB RSS")


def get_catalog(table_bucket_arn: str, region: str = "us-east-1"):
    """Get PyIceberg catalog."""
    return load_catalog(
        "s3tables",
        type="rest",
        uri=f"https://s3tables.{region}.amazonaws.com/iceberg",
        warehouse=table_bucket_arn,
        **{
            "rest.sigv4-enabled": "true",
            "rest.signing-region": region,
            "rest.signing-name": "s3tables",
        },
    )


def get_tickers_to_enrich(
    table_bucket_arn: str,
    region: str,
    updated_since: datetime | None = None,
    pharma_only: bool = True,
) -> list[tuple[str, str]]:
    """
    Get list of tickers to enrich from reference.tickers table.

    Args:
        table_bucket_arn: S3 Table Bucket ARN
        region: AWS region
        updated_since: Only include tickers updated after this time
        pharma_only: Filter to pharma-like ticker names

    Returns:
        List of (ticker, market) tuples
    """
    log("[main] Loading tickers from reference.tickers...")
    catalog = get_catalog(table_bucket_arn, region)
    tickers_table = catalog.load_table("reference.tickers")
    tickers_df = pl.from_arrow(tickers_table.scan().to_arrow())

    log(f"[main] Total tickers in table: {len(tickers_df):,}")

    # Include all tickers (active + inactive) - historical trials may reference delisted companies

    # Filter by last_updated_utc if incremental
    if updated_since:
        updated_since_str = updated_since.isoformat()
        tickers_df = tickers_df.filter(pl.col("last_updated_utc") > updated_since_str)
        log(f"[main] Tickers updated since {updated_since_str}: {len(tickers_df):,}")

    # Filter to pharma-like names
    if pharma_only:
        # Apply pharma keyword filter
        tickers_df = tickers_df.with_columns(
            pl.col("name").map_elements(is_pharma_like, return_dtype=pl.Boolean).alias("is_pharma")
        )
        tickers_df = tickers_df.filter(pl.col("is_pharma"))
        log(f"[main] Pharma-like tickers: {len(tickers_df):,}")

    # Extract (ticker, market) tuples
    result = [
        (row["ticker"], row["market"])
        for row in tickers_df.select(["ticker", "market"]).iter_rows(named=True)
    ]

    log(f"[main] Tickers to enrich: {len(result):,}")
    return result


def run(
    api_key: str | None = None,
    table_bucket_arn: str | None = None,
    region: str = "us-east-1",
    force_full: bool = False,
    last_run_time: str | None = None,
    pharma_only: bool = True,
    rate_limit_delay: float = 13.0,
    load_batch_size: int = 10,  # Small batches for frequent writes
) -> dict:
    """
    Run the ticker details enrichment pipeline.

    Args:
        api_key: Massive API key (or set MASSIVE_API_KEY env var)
        table_bucket_arn: S3 Table Bucket ARN (or set TABLE_BUCKET_ARN env var)
        region: AWS region
        force_full: Force full extraction even if we have data
        last_run_time: ISO timestamp from last pipeline run
        pharma_only: Only enrich pharma-like tickers (recommended)
        rate_limit_delay: Seconds between API calls (13s = under 5/min limit)
        load_batch_size: How often to flush to S3 Tables

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

    last_run_time = last_run_time or os.environ.get("LAST_RUN_TIME", "").strip()

    # Allow overriding rate limit delay via env var
    rate_limit_delay = float(os.environ.get("RATE_LIMIT_DELAY", rate_limit_delay))

    log("[main] Pipeline starting")
    log(f"[main] force_full={force_full}, last_run_time={last_run_time or '(not set)'}")
    log(f"[main] pharma_only={pharma_only}, rate_limit_delay={rate_limit_delay}s")
    log_memory("startup")

    # Determine incremental window
    updated_since = None
    if not force_full:
        log("[main] Checking for existing table...")
        if table_exists(table_bucket_arn, region=region):
            if last_run_time:
                updated_since = datetime.fromisoformat(last_run_time.replace("Z", "+00:00"))
                log(f"[main] Incremental mode (since: {updated_since.isoformat()})")
            else:
                log("[main] No last_run_time, will fetch all pharma-like tickers")
        else:
            log("[main] Table does not exist, full extraction mode")

    # Get tickers to enrich
    tickers = get_tickers_to_enrich(
        table_bucket_arn=table_bucket_arn,
        region=region,
        updated_since=updated_since,
        pharma_only=pharma_only,
    )

    if len(tickers) == 0:
        log("[main] No tickers to enrich")
        return {"rows_inserted": 0, "rows_updated": 0}

    # Estimate time
    est_hours = len(tickers) * rate_limit_delay / 3600
    log(
        f"[main] Estimated time: {est_hours:.1f} hours "
        f"({len(tickers)} tickers × {rate_limit_delay}s)"
    )

    # Fetch and load in batches
    total_inserted = 0
    total_updated = 0
    batch = []

    log("[main] Starting extraction and loading...")
    for detail in fetch_ticker_details_batch(
        tickers=tickers,
        api_key=api_key,
        rate_limit_delay=rate_limit_delay,
    ):
        batch.append(detail)

        # Flush batch periodically
        if len(batch) >= load_batch_size:
            log(f"[main] Flushing batch of {len(batch)} records...")
            result = load_ticker_details(
                batch,
                table_bucket_arn=table_bucket_arn,
                region=region,
            )
            total_inserted += result["rows_inserted"]
            total_updated += result["rows_updated"]
            batch = []
            log_memory("post-batch-load")

    # Final batch
    if batch:
        log(f"[main] Flushing final batch of {len(batch)} records...")
        result = load_ticker_details(
            batch,
            table_bucket_arn=table_bucket_arn,
            region=region,
        )
        total_inserted += result["rows_inserted"]
        total_updated += result["rows_updated"]

    log(f"[main] Done! {total_inserted} inserted, {total_updated} updated")
    log_memory("complete")
    return {"rows_inserted": total_inserted, "rows_updated": total_updated}


if __name__ == "__main__":
    run()
