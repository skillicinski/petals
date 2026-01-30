"""
Ticker details extraction from Massive (formerly Polygon.io) API.

Fetches detailed information for each ticker including SIC codes,
descriptions, and other metadata useful for entity matching.

Rate Limiting
=============
Massive free tier allows 5 calls/minute. This pipeline respects that limit
with configurable delays between requests. For ~3,600 pharma-like tickers,
initial backfill takes ~12 hours.

Incremental Fetching
====================
For incremental runs, we only fetch details for tickers that have been
updated since the last run (based on last_updated_utc from tickers table).
"""

import json
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Iterator


def fetch_ticker_details(
    ticker: str,
    api_key: str,
    max_retries: int = 5,
    base_delay: float = 15.0,
) -> dict | None:
    """
    Fetch detailed information for a single ticker from Massive API.

    Args:
        ticker: The ticker symbol to fetch
        api_key: Massive API key
        max_retries: Max retries on rate limit (429)
        base_delay: Base delay for exponential backoff on rate limit

    Returns:
        Dict with ticker details or None if ticker not found
    """
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={api_key}"

    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode("utf-8"))
                return data.get("results", {})
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            elif e.code == 429:
                wait = base_delay * (2**attempt)
                print(
                    f"  Rate limited on {ticker}, waiting {wait:.0f}s "
                    f"(attempt {attempt + 1}/{max_retries})..."
                )
                time.sleep(wait)
            else:
                print(f"  HTTP {e.code} fetching {ticker}: {e.reason}")
                return None
        except urllib.error.URLError as e:
            print(f"  Network error fetching {ticker}: {e.reason}")
            if attempt < max_retries - 1:
                time.sleep(base_delay)
            else:
                return None
        except Exception as e:
            print(f"  Unexpected error fetching {ticker}: {e}")
            return None

    print(f"  Max retries exceeded for {ticker}")
    return None


def _extract_detail_fields(ticker: str, market: str, details: dict) -> dict:
    """
    Extract and flatten relevant fields from API response.

    Args:
        ticker: The ticker symbol
        market: The market (from source tickers table)
        details: Raw API response

    Returns:
        Flattened dict ready for loading
    """
    # Extract address fields
    address = details.get("address", {})

    return {
        "ticker": ticker,
        "market": market,
        "name": details.get("name"),
        "sic_code": details.get("sic_code"),
        "sic_description": details.get("sic_description"),
        "description": details.get("description"),
        "homepage_url": details.get("homepage_url"),
        "market_cap": details.get("market_cap"),
        "total_employees": details.get("total_employees"),
        "list_date": details.get("list_date"),
        "locale": details.get("locale"),
        "primary_exchange": details.get("primary_exchange"),
        "currency_name": details.get("currency_name"),
        "cik": details.get("cik"),
        "composite_figi": details.get("composite_figi"),
        "share_class_figi": details.get("share_class_figi"),
        "address_city": address.get("city"),
        "address_state": address.get("state"),
        "address_country": address.get("country") if address else details.get("locale"),
        "last_fetched_utc": datetime.utcnow().isoformat() + "Z",
    }


def fetch_ticker_details_batch(
    tickers: list[tuple[str, str]],  # List of (ticker, market) tuples
    api_key: str,
    rate_limit_delay: float = 13.0,
    max_retries: int = 5,
    progress_interval: int = 10,  # Log progress every 10 tickers (~2 min)
) -> Iterator[dict]:
    """
    Fetch details for a batch of tickers, respecting rate limits.

    Args:
        tickers: List of (ticker, market) tuples to fetch
        api_key: Massive API key
        rate_limit_delay: Seconds between requests (13s keeps us under 5/min)
        max_retries: Max retries per ticker on rate limit
        progress_interval: How often to log progress

    Yields:
        Dict with ticker details for each successful fetch
    """
    total = len(tickers)
    fetched = 0
    skipped = 0

    print(f"Fetching details for {total} tickers...")
    print(f"Rate limit delay: {rate_limit_delay}s between requests")
    print(f"Estimated time: {total * rate_limit_delay / 3600:.1f} hours")

    for i, (ticker, market) in enumerate(tickers):
        if i > 0:
            time.sleep(rate_limit_delay)

        details = fetch_ticker_details(ticker, api_key, max_retries=max_retries)

        if details:
            yield _extract_detail_fields(ticker, market, details)
            fetched += 1
        else:
            skipped += 1

        if (i + 1) % progress_interval == 0 or i == total - 1:
            elapsed_pct = 100 * (i + 1) / total
            print(
                f"Progress: {i + 1}/{total} ({elapsed_pct:.1f}%) "
                f"- fetched: {fetched}, skipped: {skipped}"
            )

    print(f"Fetch complete: {fetched} fetched, {skipped} skipped")


# Keywords to identify pharma/biotech/medical tickers
PHARMA_KEYWORDS = [
    "pharm",
    "bio",
    "thera",
    "medic",
    "health",
    "drug",
    "onco",
    "immun",
    "genetic",
    "genom",
    "diagnos",
    "clinic",
    "surgical",
    "dental",
    "ophthalm",
    "cardio",
    "neuro",
]


def is_pharma_like(name: str | None) -> bool:
    """Check if a ticker name suggests pharma/biotech/medical company."""
    if not name:
        return False
    name_lower = name.lower()
    return any(kw in name_lower for kw in PHARMA_KEYWORDS)


if __name__ == "__main__":
    import os

    api_key = os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        print("MASSIVE_API_KEY environment variable required")
        exit(1)

    # Test with a few known tickers
    test_tickers = [("PFE", "stocks"), ("JNJ", "stocks"), ("INVALID123", "stocks")]

    for record in fetch_ticker_details_batch(test_tickers, api_key, rate_limit_delay=1.0):
        print(f"{record['ticker']}: SIC={record['sic_code']} - {record['name'][:40]}...")
