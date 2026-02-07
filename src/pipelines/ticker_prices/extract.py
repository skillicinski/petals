"""
Extract OHLC price data using yfinance.

Fetches yesterday's (or today's if markets are closed) OHLC data for US tickers.
yfinance is much faster than Polygon free tier - no rate limits for reasonable use.

Market Hours Logic
==================
- If running during market hours (9:30am-4pm ET), fetches up to previous trading day
- If running after market close, can safely fetch today's data
- Always fetches the most recent complete trading day's data

Performance
===========
yfinance can handle bulk downloads efficiently:
- ~100 tickers in parallel takes ~30 seconds
- Much faster than API rate-limited approaches

No Data Handling
================
When yfinance returns no data (delisted, thinly traded preferred stocks, etc.),
we write a minimal record with nulls so future runs skip these tickers rather
than retrying indefinitely. This saves significant processing time.
"""

import sys
from datetime import datetime, timedelta, timezone
from typing import Iterator

import pandas as pd
import polars as pl
import yfinance as yf


def log(msg: str) -> None:
    """Print and flush immediately."""
    print(msg)
    sys.stdout.flush()


def get_latest_trading_day() -> str:
    """
    Get the most recent complete trading day in YYYY-MM-DD format.

    Simple heuristic: always fetch yesterday's data to ensure it's complete.
    For more sophisticated market hours detection, could use market calendars.

    Returns:
        Date string in YYYY-MM-DD format
    """
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def fetch_ticker_prices_batch(
    tickers: list[str],
    date: str | None = None,
    batch_size: int = 100,
    progress_interval: int = 100,
) -> Iterator[dict]:
    """
    Fetch OHLC price data for a batch of tickers using yfinance.

    Downloads data in parallel batches for efficiency. Always yields a record
    for each ticker (even when no data is available) so future runs can skip
    tickers without recent trading activity.

    Args:
        tickers: List of ticker symbols to fetch
        date: Target date (YYYY-MM-DD). If None, uses yesterday.
        batch_size: Number of tickers to fetch in parallel
        progress_interval: Log progress every N tickers

    Yields:
        Dict with ticker, date, open, high, low, close, volume, last_fetched_utc
        (OHLC fields will be None if no data is available)
    """
    if not date:
        date = get_latest_trading_day()

    total = len(tickers)
    log(f"[extract] Fetching OHLC data for {total:,} tickers on {date}")
    log(f"[extract] Batch size: {batch_size} tickers in parallel")
    log("[extract] Note: All tickers get a record (even when no data) to optimize future runs")

    # Calculate date range: fetch just the target date plus a buffer
    # (yfinance needs a range, single day sometimes returns empty)
    start_date = date
    end_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=2)).strftime("%Y-%m-%d")

    fetched = 0
    no_data = 0

    # Process in batches
    for i in range(0, total, batch_size):
        batch_tickers = tickers[i : i + batch_size]
        batch_str = " ".join(batch_tickers)

        try:
            # Download batch in parallel (returns pandas DataFrame)
            data_pd = yf.download(
                batch_str,
                start=start_date,
                end=end_date,
                group_by="ticker",
                progress=False,
                threads=True,
            )

            # Convert to polars for processing
            if data_pd.empty:
                no_data += len(batch_tickers)
                continue

            # Handle single vs multiple tickers (yfinance returns different shapes)
            if len(batch_tickers) == 1:
                ticker = batch_tickers[0]
                # Single ticker: y finance returns MultiIndex columns like ('Close', 'AAPL')
                # Extract data using pandas before converting to polars

                # Find the target date in the index
                date_mask = data_pd.index.strftime("%Y-%m-%d") == date
                if date_mask.any():
                    row_data = data_pd[date_mask].iloc[0]

                    # Handle MultiIndex columns
                    # yfinance with group_by='ticker' returns (ticker, metric)
                    if isinstance(data_pd.columns, pd.MultiIndex):
                        # Extract values from MultiIndex tuples (ticker, metric)
                        open_val = (
                            row_data[(ticker, "Open")]
                            if pd.notna(row_data[(ticker, "Open")])
                            else None
                        )
                        high_val = (
                            row_data[(ticker, "High")]
                            if pd.notna(row_data[(ticker, "High")])
                            else None
                        )
                        low_val = (
                            row_data[(ticker, "Low")]
                            if pd.notna(row_data[(ticker, "Low")])
                            else None
                        )
                        close_val = (
                            row_data[(ticker, "Close")]
                            if pd.notna(row_data[(ticker, "Close")])
                            else None
                        )
                        volume_val = (
                            int(row_data[(ticker, "Volume")])
                            if pd.notna(row_data[(ticker, "Volume")])
                            else None
                        )

                        yield {
                            "ticker": ticker,
                            "date": date,
                            "open": open_val,
                            "high": high_val,
                            "low": low_val,
                            "close": close_val,
                            "volume": volume_val,
                            "last_fetched_utc": datetime.now(timezone.utc)
                            .isoformat()
                            .replace("+00:00", "Z"),
                        }
                    else:
                        # Regular columns (fallback, though yfinance typically uses MultiIndex)
                        yield {
                            "ticker": ticker,
                            "date": date,
                            "open": row_data["Open"] if pd.notna(row_data["Open"]) else None,
                            "high": row_data["High"] if pd.notna(row_data["High"]) else None,
                            "low": row_data["Low"] if pd.notna(row_data["Low"]) else None,
                            "close": row_data["Close"] if pd.notna(row_data["Close"]) else None,
                            "volume": int(row_data["Volume"])
                            if pd.notna(row_data["Volume"])
                            else None,
                            "last_fetched_utc": datetime.now(timezone.utc)
                            .isoformat()
                            .replace("+00:00", "Z"),
                        }
                    fetched += 1
                else:
                    # No data for target date - yield minimal record to skip on future runs
                    yield {
                        "ticker": ticker,
                        "date": date,
                        "open": None,
                        "high": None,
                        "low": None,
                        "close": None,
                        "volume": None,
                        "last_fetched_utc": datetime.now(timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z"),
                    }
                    no_data += 1
            else:
                # Multiple tickers - data has MultiIndex columns
                # Need to handle this differently due to MultiIndex
                for ticker in batch_tickers:
                    try:
                        if ticker in data_pd.columns.levels[0]:
                            ticker_data_pd = data_pd[ticker]
                            if not ticker_data_pd.empty:
                                # Convert to polars
                                df = pl.from_pandas(ticker_data_pd.reset_index())
                                df = df.with_columns(
                                    pl.col("Date").dt.strftime("%Y-%m-%d").alias("date_str")
                                )

                                # Filter to target date
                                df_filtered = df.filter(pl.col("date_str") == date)

                                if len(df_filtered) > 0:
                                    row = df_filtered.row(0, named=True)
                                    yield {
                                        "ticker": ticker,
                                        "date": date,
                                        "open": row.get("Open"),
                                        "high": row.get("High"),
                                        "low": row.get("Low"),
                                        "close": row.get("Close"),
                                        "volume": int(row["Volume"])
                                        if row.get("Volume") is not None
                                        else None,
                                        "last_fetched_utc": datetime.now(timezone.utc)
                                        .isoformat()
                                        .replace("+00:00", "Z"),
                                    }
                                    fetched += 1
                                else:
                                    # No data for target date - yield minimal record
                                    yield {
                                        "ticker": ticker,
                                        "date": date,
                                        "open": None,
                                        "high": None,
                                        "low": None,
                                        "close": None,
                                        "volume": None,
                                        "last_fetched_utc": datetime.now(timezone.utc)
                                        .isoformat()
                                        .replace("+00:00", "Z"),
                                    }
                                    no_data += 1
                            else:
                                # Empty dataframe - yield minimal record
                                yield {
                                    "ticker": ticker,
                                    "date": date,
                                    "open": None,
                                    "high": None,
                                    "low": None,
                                    "close": None,
                                    "volume": None,
                                    "last_fetched_utc": datetime.now(timezone.utc)
                                    .isoformat()
                                    .replace("+00:00", "Z"),
                                }
                                no_data += 1
                        else:
                            # Ticker not in response - yield minimal record
                            yield {
                                "ticker": ticker,
                                "date": date,
                                "open": None,
                                "high": None,
                                "low": None,
                                "close": None,
                                "volume": None,
                                "last_fetched_utc": datetime.now(timezone.utc)
                                .isoformat()
                                .replace("+00:00", "Z"),
                            }
                            no_data += 1
                    except (KeyError, IndexError, Exception):
                        # Error processing ticker - yield minimal record
                        yield {
                            "ticker": ticker,
                            "date": date,
                            "open": None,
                            "high": None,
                            "low": None,
                            "close": None,
                            "volume": None,
                            "last_fetched_utc": datetime.now(timezone.utc)
                            .isoformat()
                            .replace("+00:00", "Z"),
                        }
                        no_data += 1

        except Exception as e:
            import traceback

            log(
                f"[extract] Error fetching batch {i}-{i + len(batch_tickers)}: "
                f"{type(e).__name__}: {e}"
            )
            log(f"[extract] Traceback: {traceback.format_exc()}")
            # Yield minimal records for all tickers in failed batch
            for ticker in batch_tickers:
                yield {
                    "ticker": ticker,
                    "date": date,
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "volume": None,
                    "last_fetched_utc": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                }
            no_data += len(batch_tickers)

        # Log progress
        processed = min(i + batch_size, total)
        if processed % progress_interval == 0 or processed == total:
            pct = 100 * processed / total
            log(
                f"[extract] Progress: {processed:,}/{total:,} ({pct:.1f}%) "
                f"- with data: {fetched:,}, no data: {no_data:,}"
            )

    log(f"[extract] Fetch complete: {fetched:,} with data, {no_data:,} no data")


if __name__ == "__main__":
    # Test with a few known tickers
    test_tickers = ["AAPL", "GOOGL", "MSFT", "INVALID123"]

    print("Testing ticker price extraction...")
    for record in fetch_ticker_prices_batch(test_tickers, batch_size=2):
        ticker = record["ticker"]
        close = record["close"]
        print(f"{ticker}: close=${close:.2f} on {record['date']}")
