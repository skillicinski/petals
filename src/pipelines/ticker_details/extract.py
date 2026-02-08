"""
Ticker details extraction using yfinance.

Fetches detailed information for each ticker including industry, sector,
descriptions, and other metadata useful for entity matching.

Rate Limiting
=============
yfinance has no official rate limits, making this much faster than the
previous Polygon API approach. For ~15,000 US stock tickers, extraction
takes approximately 2 hours vs 54 hours with Polygon.

Incremental Fetching
====================
For incremental runs, we only fetch details for tickers that have been
updated since the last run (based on last_updated_utc from tickers table).

Error Handling
==============
When yfinance cannot fetch ticker info (delisted, invalid, etc.), we write
a minimal record with only ticker, market, and last_fetched_utc. This prevents
re-attempting failed tickers on future runs.
"""

import time
from datetime import datetime, UTC
from typing import Iterator

import yfinance as yf


def fetch_ticker_details(ticker: str) -> dict | None:
    """
    Fetch detailed information for a single ticker using yfinance.

    Args:
        ticker: The ticker symbol to fetch

    Returns:
        Dict with ticker info or None if ticker not found/error
    """
    try:
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.info

        # Check if we got valid data (yfinance returns empty dict or minimal data on error)
        if not info or not info.get("symbol"):
            return None

        return info
    except Exception as e:
        print(f"  Error fetching {ticker}: {e}")
        return None


def _extract_detail_fields(ticker: str, market: str, info: dict) -> dict:
    """
    Extract and flatten relevant fields from yfinance info dict.

    Args:
        ticker: The ticker symbol
        market: The market (from source tickers table)
        info: yfinance info dict

    Returns:
        Flattened dict ready for loading
    """
    return {
        "ticker": ticker,
        "market": market,
        "name": info.get("longName") or info.get("shortName"),
        "description": info.get("longBusinessSummary"),
        "homepage_url": info.get("website"),
        "market_cap": info.get("marketCap"),
        "total_employees": info.get("fullTimeEmployees"),
        "locale": info.get("country"),
        "primary_exchange": info.get("exchange"),
        "currency_name": info.get("currency"),
        "address_city": info.get("city"),
        "address_state": info.get("state"),
        "address_country": info.get("country"),
        "industry": info.get("industry"),
        "sector": info.get("sector"),
        "last_fetched_utc": datetime.now(UTC).isoformat(),
    }


def fetch_ticker_details_batch(
    tickers: list[tuple[str, str]],  # List of (ticker, market) tuples
    progress_interval: int = 100,  # Log progress every 100 tickers
    batch_delay: float = 0.5,  # Small delay between requests to be respectful
) -> Iterator[dict]:
    """
    Fetch details for a batch of tickers using yfinance.

    Always yields a record for each ticker (even on error) so that future runs
    can skip tickers where data is unavailable.

    Args:
        tickers: List of (ticker, market) tuples to fetch
        progress_interval: How often to log progress
        batch_delay: Seconds between requests (small delay to be respectful)

    Yields:
        Dict with ticker details (or minimal record on error)
    """
    total = len(tickers)
    fetched = 0
    skipped = 0

    print(f"Fetching details for {total} tickers using yfinance...")
    print(f"Estimated time: {total * batch_delay / 60:.1f} minutes")
    print("Note: All tickers get a record (even errors) to optimize future runs")

    for i, (ticker, market) in enumerate(tickers):
        if i > 0 and batch_delay > 0:
            time.sleep(batch_delay)

        info = fetch_ticker_details(ticker)

        if info:
            yield _extract_detail_fields(ticker, market, info)
            fetched += 1
        else:
            # Yield minimal record (error/not found) so we skip this ticker on future runs
            yield {
                "ticker": ticker,
                "market": market,
                "name": None,
                "description": None,
                "homepage_url": None,
                "market_cap": None,
                "total_employees": None,
                "locale": None,
                "primary_exchange": None,
                "currency_name": None,
                "address_city": None,
                "address_state": None,
                "address_country": None,
                "industry": None,
                "sector": None,
                "last_fetched_utc": datetime.now(UTC).isoformat(),
            }
            skipped += 1

        if (i + 1) % progress_interval == 0 or i == total - 1:
            elapsed_pct = 100 * (i + 1) / total
            print(
                f"Progress: {i + 1}/{total} ({elapsed_pct:.1f}%) "
                f"- with details: {fetched}, no details: {skipped}"
            )

    print(f"Fetch complete: {fetched} with details, {skipped} not found")


if __name__ == "__main__":
    # Test with a few known tickers
    test_tickers = [
        ("PFE", "stocks"),
        ("JNJ", "stocks"),
        ("MRNA", "stocks"),
        ("INVALID123", "stocks"),
    ]

    print("Testing yfinance extraction:")
    for record in fetch_ticker_details_batch(test_tickers, batch_delay=0.5):
        name = record.get("name") or "N/A"
        industry = record.get("industry") or "N/A"
        if name != "N/A" and len(name) > 40:
            name = name[:40] + "..."
        print(f"{record['ticker']:10s}: {name:45s} | {industry}")
