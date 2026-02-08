"""
Main entry point for ticker details enrichment pipeline.

Reads tickers from market.tickers table, fetches detailed information
from Massive API, and loads to market.ticker_details table.

Filters to US stock tickers only. Industry filtering (e.g., pharma/biotech)
is handled downstream in entity_match using SIC codes.

Change Detection:
- Compares last_updated_utc (tickers) vs last_fetched_utc (ticker_details)
- Only fetches details for tickers that are new or have been updated
- No separate "incremental" mode needed - always efficient

Rate Limiting:
- Respects Massive free tier limit (5 calls/min)
- Initial backfill for ~15,000 US stock tickers takes ~54 hours
- Subsequent runs only fetch changed tickers (typically minutes)
"""

import faulthandler
import os
import sys

import polars as pl
import psutil
from pyiceberg.catalog import load_catalog

from .extract import fetch_ticker_details_batch
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
) -> list[tuple[str, str]]:
    """
    Get list of tickers to enrich from market.tickers table.

    Filters to US stock tickers only. Only returns tickers that:
    - Don't exist in ticker_details yet, OR
    - Have been updated since their details were last fetched

    Industry filtering (pharma/biotech) is handled downstream using SIC codes.

    Args:
        table_bucket_arn: S3 Table Bucket ARN
        region: AWS region

    Returns:
        List of (ticker, market) tuples
    """
    log("[main] Loading tickers from market.tickers...")
    catalog = get_catalog(table_bucket_arn, region)
    tickers_table = catalog.load_table("market.tickers")
    tickers_df = pl.from_arrow(tickers_table.scan().to_arrow())

    log(f"[main] Total tickers in table: {len(tickers_df):,}")

    # Filter to US stocks only (includes active + inactive for historical trial matching)
    tickers_df = tickers_df.filter((pl.col("market") == "stocks") & (pl.col("locale") == "us"))
    log(f"[main] US stock tickers: {len(tickers_df):,}")

    # Compare against existing ticker_details to skip tickers that haven't changed
    if table_exists(table_bucket_arn, region=region):
        log("[main] Loading existing ticker_details for change detection...")
        details_table = catalog.load_table("market.ticker_details")
        details_df = pl.from_arrow(
            details_table.scan(selected_fields=("ticker", "market", "last_fetched_utc")).to_arrow()
        )
        log(f"[main] Existing ticker_details records: {len(details_df):,}")

        if len(details_df) == 0:
            log(
                "[main] ticker_details is empty; will enrich all US tickers "
                "(first run or table was recreated)"
            )

        # One row per (ticker, market): keep latest last_fetched_utc in case of duplicates
        details_df = details_df.group_by(["ticker", "market"]).agg(
            pl.col("last_fetched_utc").max().alias("last_fetched_utc")
        )

        # Left join tickers with details
        tickers_df = tickers_df.join(details_df, on=["ticker", "market"], how="left")

        # Compare as strings so type drift (e.g. timestamp vs string) doesn't break the filter
        last_updated_str = pl.col("last_updated_utc").cast(pl.Utf8)
        last_fetched_str = pl.col("last_fetched_utc").cast(pl.Utf8)

        # Keep tickers where:
        # - No details exist (last_fetched_utc is null), OR
        # - Ticker updated after details were fetched
        before_count = len(tickers_df)
        tickers_df = tickers_df.filter(
            pl.col("last_fetched_utc").is_null() | (last_updated_str > last_fetched_str)
        )
        skipped = before_count - len(tickers_df)
        log(f"[main] Skipping {skipped:,} tickers with up-to-date details")
    else:
        log(
            "[main] ticker_details table not found; will enrich all US tickers (table_exists=False)"
        )

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
    rate_limit_delay: float = 13.0,
    load_batch_size: int = 10,  # Small batches for frequent writes
    **_kwargs,  # Accept but ignore force_full, last_run_time for backwards compat
) -> dict:
    """
    Run the ticker details enrichment pipeline.

    Automatically determines which tickers need enrichment by comparing
    last_updated_utc (from tickers table) against last_fetched_utc (from
    ticker_details table). Only fetches details for new or updated tickers.

    Args:
        api_key: Massive API key (or set MASSIVE_API_KEY env var)
        table_bucket_arn: S3 Table Bucket ARN (or set TABLE_BUCKET_ARN env var)
        region: AWS region
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

    # Allow overriding rate limit delay via env var
    rate_limit_delay = float(os.environ.get("RATE_LIMIT_DELAY", rate_limit_delay))

    log("[main] Pipeline starting")
    log(f"[main] rate_limit_delay={rate_limit_delay}s")
    log_memory("startup")

    # Get tickers to enrich (automatically skips tickers with up-to-date details)
    tickers = get_tickers_to_enrich(
        table_bucket_arn=table_bucket_arn,
        region=region,
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
