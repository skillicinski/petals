"""
Main entry point for ticker prices (OHLC) pipeline.

Reads US stock tickers from reference.tickers table and fetches
yesterday's (or today's if after market close) OHLC data using yfinance.
Loads to market_data.ticker_prices table.

Performance
===========
Unlike the Polygon API (5 calls/min), yfinance is much faster:
- Can fetch ~100 tickers in parallel in ~30 seconds
- Entire US market (~15,000 tickers) in ~10-15 minutes
- No rate limiting for reasonable use

Daily Updates
=============
This pipeline is designed to run daily to capture the most recent
trading day's data. Can be scheduled to run:
- After market close (4pm ET) to get same-day data
- In the morning to get previous day's data

No Data Handling
================
Tickers without available price data (delisted, thinly traded preferreds)
are written with null OHLC values. This creates a record preventing wasted
re-fetch attempts. Historical prices are immutable, so no change detection
is needed - each run fetches for a specific date.
"""

import os
import sys

import polars as pl
from pyiceberg.catalog import load_catalog

from .extract import fetch_ticker_prices_batch, get_latest_trading_day
from .load import load_ticker_prices


def log(msg: str) -> None:
    """Print and flush immediately."""
    print(msg)
    sys.stdout.flush()


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


def get_us_stock_tickers(
    table_bucket_arn: str,
    region: str,
    date: str,
    active_only: bool = True,
) -> list[str]:
    """
    Get list of US stock tickers to fetch prices for.

    Filters to tickers that either:
    - Don't have prices for the target date yet, OR
    - Have a null close price (no data was available previously)

    Args:
        table_bucket_arn: S3 Table Bucket ARN
        region: AWS region
        date: Target date (YYYY-MM-DD)
        active_only: If True, only return active tickers (default)

    Returns:
        List of ticker symbols
    """
    log("[main] Loading tickers from reference.tickers...")
    catalog = get_catalog(table_bucket_arn, region)
    tickers_table = catalog.load_table("reference.tickers")
    tickers_df = pl.from_arrow(tickers_table.scan().to_arrow())

    log(f"[main] Total tickers in table: {len(tickers_df):,}")

    # Filter to US stocks
    tickers_df = tickers_df.filter((pl.col("market") == "stocks") & (pl.col("locale") == "us"))
    log(f"[main] US stock tickers: {len(tickers_df):,}")

    # Optionally filter to active only
    if active_only:
        tickers_df = tickers_df.filter(pl.col("active"))
        log(f"[main] Active US stock tickers: {len(tickers_df):,}")

    # Extract ticker symbols
    tickers = tickers_df["ticker"].to_list()
    log(f"[main] Tickers to fetch prices for: {len(tickers):,}")

    return tickers


def run(
    table_bucket_arn: str | None = None,
    region: str = "us-east-1",
    date: str | None = None,
    active_only: bool = True,
    fetch_batch_size: int = 100,
    load_batch_size: int = 1000,
    **_kwargs,
) -> dict:
    """
    Run the ticker prices pipeline.

    Fetches OHLC data for all US stock tickers and loads to Iceberg table.

    Args:
        table_bucket_arn: S3 Table Bucket ARN (or set TABLE_BUCKET_ARN env var)
        region: AWS region
        date: Target date (YYYY-MM-DD). If None, uses yesterday.
        active_only: Only fetch prices for active tickers (default: True)
        fetch_batch_size: Number of tickers to fetch in parallel per batch
        load_batch_size: Number of rows per database upsert batch

    Returns:
        Stats dict with rows_inserted and rows_updated counts
    """
    table_bucket_arn = table_bucket_arn or os.environ.get("TABLE_BUCKET_ARN")
    if not table_bucket_arn:
        raise ValueError("TABLE_BUCKET_ARN required")

    # Determine target date
    if not date:
        date = get_latest_trading_day()

    log("[main] Pipeline starting")
    log(f"[main] Target date: {date}")
    log(f"[main] Active tickers only: {active_only}")
    log(f"[main] Fetch batch size: {fetch_batch_size}")
    log(f"[main] Load batch size: {load_batch_size}")

    # Get tickers to fetch
    tickers = get_us_stock_tickers(
        table_bucket_arn=table_bucket_arn,
        region=region,
        active_only=active_only,
    )

    if len(tickers) == 0:
        log("[main] No tickers to fetch")
        return {"rows_inserted": 0, "rows_updated": 0}

    # Estimate time
    est_minutes = len(tickers) / fetch_batch_size * 0.5  # ~30s per batch of 100
    log(f"[main] Estimated time: {est_minutes:.1f} minutes")

    # Fetch prices
    log("[main] Starting price extraction...")
    prices = []
    for price_record in fetch_ticker_prices_batch(
        tickers=tickers,
        date=date,
        batch_size=fetch_batch_size,
    ):
        prices.append(price_record)

    log(f"[main] Extraction complete: {len(prices):,} price records")

    if len(prices) == 0:
        log("[main] No price data fetched")
        return {"rows_inserted": 0, "rows_updated": 0}

    # Load to Iceberg
    log("[main] Starting load to Iceberg...")
    result = load_ticker_prices(
        prices,
        table_bucket_arn=table_bucket_arn,
        region=region,
        batch_size=load_batch_size,
    )

    log(f"[main] Done! {result['rows_inserted']} inserted, {result['rows_updated']} updated")
    return result


if __name__ == "__main__":
    run()
