"""
Ticker list extraction from Massive (formerly Polygon.io) API.

Fetches tickers from the bulk endpoint and yields them as dicts.

Incremental Fetching
====================
For incremental runs (table already exists), we:
1. Order by `last_updated_utc` descending (newest first)
2. Stop fetching when we hit records older than the cutoff timestamp
3. This reduces runtime from ~15 min to seconds for most runs

For initial loads (table doesn't exist), we fetch all tickers.

Design Decision: SCD Type 1 with API-provided delisting dates
=============================================================
We use SCD Type 1 (update in place) with:
- `delisted_utc` from the bulk API response as the authoritative delisting date
- `active` boolean to indicate current trading status
- `last_updated_utc` for incremental change detection

The trade-off is we don't track exactly when a ticker first appeared in our system,
but `list_date` is available via the detail endpoint if ever needed for specific tickers.
"""

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Iterator


def _fetch_tickers_paginated(
    api_key: str,
    active: bool,
    limit: int,
    updated_since: datetime | None,
    rate_limit_delay: float,
    max_retries: int,
) -> Iterator[dict]:
    """
    Internal: fetch tickers with a specific active filter, handling pagination.

    Args:
        api_key: Massive API key
        active: Filter for active (True) or inactive/delisted (False) tickers
        limit: Results per page (max 1000)
        updated_since: Only fetch tickers updated after this timestamp
        rate_limit_delay: Base seconds to wait between requests
        max_retries: Max consecutive rate limit retries before giving up
    """
    base_url = "https://api.polygon.io/v3/reference/tickers"
    active_str = "true" if active else "false"
    label = "active" if active else "inactive"

    # Build URL with active filter and ordering for incremental fetches
    params = f"limit={limit}&active={active_str}"
    if updated_since:
        # Order by last_updated_utc descending to get newest first
        params += "&order=desc&sort=last_updated_utc"

    url = f"{base_url}?{params}&apiKey={api_key}"
    page = 0
    total_yielded = 0
    cutoff_reached = False

    while url and not cutoff_reached:
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
                        f"[{label}] Rate limited (attempt {retries}/{max_retries})"
                        f", waiting {wait_time:.0f}s..."
                    )
                    time.sleep(wait_time)
                else:
                    raise

        if data is None:
            raise RuntimeError(f"[{label}] Max retries ({max_retries}) exceeded on page {page}")

        results = data.get("results", [])
        page_yielded = 0

        for ticker in results:
            # Check if we've reached the cutoff (incremental mode)
            if updated_since:
                last_updated = ticker.get("last_updated_utc")
                if not last_updated:
                    # Skip tickers without timestamps in incremental mode
                    # (e.g., indices which have no last_updated_utc)
                    continue

                # Parse ISO timestamp (handles both with and without Z suffix)
                try:
                    ticker_updated = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
                    # Make updated_since timezone-aware if needed
                    if updated_since.tzinfo is None:
                        ticker_updated = ticker_updated.replace(tzinfo=None)

                    if ticker_updated < updated_since:
                        cutoff_reached = True
                        print(f"[{label}] Reached cutoff at {last_updated}, stopping fetch")
                        break
                except ValueError:
                    pass  # Skip timestamp parsing errors, include the record

            yield ticker
            total_yielded += 1
            page_yielded += 1

        page += 1
        print(f"[{label}] Page {page}: fetched {page_yielded} tickers (total: {total_yielded})")

        if cutoff_reached:
            break

        # Handle pagination
        next_url = data.get("next_url")
        if next_url:
            url = f"{next_url}&apiKey={api_key}"
            time.sleep(rate_limit_delay)  # Respect rate limits between pages
        else:
            url = None

    print(f"[{label}] Fetch complete: {total_yielded} tickers")


def fetch_tickers(
    api_key: str,
    limit: int = 1000,
    updated_since: datetime | None = None,
    rate_limit_delay: float = 15.0,
    max_retries: int = 10,
    include_inactive: bool = True,
) -> Iterator[dict]:
    """
    Fetch tickers from Massive API, handling pagination.

    Fetches both active and inactive (delisted) tickers by default.
    The API requires separate calls for active=true and active=false.

    For incremental fetching, pass `updated_since` to only fetch tickers
    updated after that timestamp. Results are ordered by last_updated_utc
    descending, and fetching stops when older records are encountered.

    Uses stdlib only (urllib).

    Yields dicts with keys including:
        ticker, name, market, locale, primary_exchange, type, active,
        currency_name, cik, composite_figi, delisted_utc, last_updated_utc

    Args:
        api_key: Massive API key
        limit: Results per page (max 1000)
        updated_since: Only fetch tickers updated after this timestamp (incremental mode)
        rate_limit_delay: Base seconds to wait between requests (free tier = 5/min)
        max_retries: Max consecutive rate limit retries before giving up
        include_inactive: Also fetch inactive/delisted tickers (default: True)
    """
    if updated_since:
        print(f"Incremental mode: fetching tickers updated since {updated_since.isoformat()}")
    else:
        print("Full extraction mode: fetching all tickers")

    # Fetch active tickers
    print("Fetching active tickers...")
    yield from _fetch_tickers_paginated(
        api_key=api_key,
        active=True,
        limit=limit,
        updated_since=updated_since,
        rate_limit_delay=rate_limit_delay,
        max_retries=max_retries,
    )

    # Fetch inactive/delisted tickers
    if include_inactive:
        print("Fetching inactive/delisted tickers...")
        yield from _fetch_tickers_paginated(
            api_key=api_key,
            active=False,
            limit=limit,
            updated_since=updated_since,
            rate_limit_delay=rate_limit_delay,
            max_retries=max_retries,
        )


def main():
    """Fetch tickers and print count (for testing)."""
    api_key = os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        raise ValueError("MASSIVE_API_KEY environment variable required")

    tickers = list(fetch_tickers(api_key))
    active_count = sum(1 for t in tickers if t.get("active"))
    inactive_count = len(tickers) - active_count

    print(f"Fetched {len(tickers)} tickers ({active_count} active, {inactive_count} inactive)")

    # Show samples
    if tickers:
        active_sample = next((t for t in tickers if t.get("active")), None)
        inactive_sample = next((t for t in tickers if not t.get("active")), None)
        if active_sample:
            print(f"Active sample: {active_sample.get('ticker')} - {active_sample.get('name')}")
        if inactive_sample:
            ticker, name = inactive_sample.get("ticker"), inactive_sample.get("name")
            print(f"Inactive sample: {ticker} - {name}")


if __name__ == "__main__":
    main()
