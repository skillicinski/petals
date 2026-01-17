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
    api_key: str, limit: int = 1000, rate_limit_delay: float = 12.0
) -> Iterator[dict]:
    """
    Fetch all tickers from Massive API, handling pagination.
    
    Uses stdlib only (urllib).
    
    Args:
        api_key: Massive API key
        limit: Results per page (max 1000)
        rate_limit_delay: Seconds to wait between requests (free tier = 5/min)
    """
    base_url = "https://api.polygon.io/v3/reference/tickers"
    url = f"{base_url}?limit={limit}&apiKey={api_key}"
    
    while url:
        req = urllib.request.Request(url)
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print(f"Rate limited, waiting {rate_limit_delay}s...")
                time.sleep(rate_limit_delay)
                continue
            raise
        
        for ticker in data.get("results", []):
            yield ticker
        
        # Handle pagination
        next_url = data.get("next_url")
        if next_url:
            url = f"{next_url}&apiKey={api_key}"
            time.sleep(rate_limit_delay)  # Respect rate limits
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
