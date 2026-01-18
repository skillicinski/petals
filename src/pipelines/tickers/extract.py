"""
Ticker list extraction from Massive (formerly Polygon.io) API.

Fetches all tickers and writes to S3 Tables as an Iceberg table.
"""

import json
import os
import time
import urllib.error
import urllib.request
from typing import Iterator


def fetch_tickers(
    api_key: str,
    limit: int = 1000,
    rate_limit_delay: float = 15.0,
    max_retries: int = 10,
) -> Iterator[dict]:
    """
    Fetch all tickers from Massive API, handling pagination.

    Uses stdlib only (urllib).

    Args:
        api_key: Massive API key
        limit: Results per page (max 1000)
        rate_limit_delay: Base seconds to wait between requests (free tier = 5/min)
        max_retries: Max consecutive rate limit retries before giving up
    """
    base_url = "https://api.polygon.io/v3/reference/tickers"
    url = f"{base_url}?limit={limit}&apiKey={api_key}"
    page = 0
    total_yielded = 0

    while url:
        retries = 0
        data = None

        while retries < max_retries:
            req = urllib.request.Request(url)
            try:
                with urllib.request.urlopen(req) as response:
                    data = json.loads(response.read().decode("utf-8"))
                break  # Success, exit retry loop
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    retries += 1
                    wait_time = rate_limit_delay * (2 ** (retries - 1))  # Exponential backoff
                    print(
                        f"Rate limited (attempt {retries}/{max_retries})"
                        f", waiting {wait_time:.0f}s..."
                    )
                    time.sleep(wait_time)
                else:
                    raise

        if data is None:
            raise RuntimeError(f"Max retries ({max_retries}) exceeded on page {page}")

        results = data.get("results", [])
        for ticker in results:
            yield ticker
            total_yielded += 1

        page += 1
        print(f"Page {page}: fetched {len(results)} tickers (total: {total_yielded})")

        # Handle pagination
        next_url = data.get("next_url")
        if next_url:
            url = f"{next_url}&apiKey={api_key}"
            time.sleep(rate_limit_delay)  # Respect rate limits between pages
        else:
            url = None


def main():
    """Fetch tickers and print count (for testing)."""
    api_key = os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        raise ValueError("MASSIVE_API_KEY environment variable required")

    tickers = list(fetch_tickers(api_key))
    print(f"Fetched {len(tickers)} tickers")

    # Show sample
    if tickers:
        print(f"Sample: {tickers[0]}")


if __name__ == "__main__":
    main()
