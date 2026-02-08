"""Load data for entity resolution analysis.

Loads entities from S3 Tables warehouse for local analysis and model development.
This is the data access layer for the entity resolution analytics workload.

Data Sources:
-------------
- Sponsors: clinical.trials table (sponsor entities from clinical trials)
- Tickers: market.ticker_details table (public company information)

Unlike pipelines (which move/transform data), this module loads data from the
warehouse into memory for exploratory analysis and ML model development.
"""

import os

import polars as pl
from pyiceberg.catalog import load_catalog


def _get_catalog():
    """Get PyIceberg catalog for S3 Tables warehouse.

    This is an internal helper - analytics code should use the load_* functions.
    """
    bucket_arn = os.environ.get(
        "TABLE_BUCKET_ARN",
        "arn:aws:s3tables:us-east-1:620117234001:bucket/petals-tables-620117234001",
    )
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    return load_catalog(
        "s3tables",
        type="rest",
        uri=f"https://s3tables.{region}.amazonaws.com/iceberg",
        warehouse=bucket_arn,
        **{
            "rest.sigv4-enabled": "true",
            "rest.signing-region": region,
            "rest.signing-name": "s3tables",
        },
    )


def load_sponsors(
    limit: int | None = None,
    min_trials: int = 1,
) -> pl.DataFrame:
    """Load unique sponsor names from clinical trials warehouse.

    Queries the clinical.trials table and returns distinct sponsors.
    Useful for entity resolution analysis - matching these sponsors to
    public companies.

    Args:
        limit: Maximum number of sponsors (for development/testing)
        min_trials: Minimum number of trials (default: 1, all sponsors)

    Returns:
        DataFrame with columns:
        - sponsor_name: str - Unique sponsor entity name

    Note:
        For development, use limit to work with smaller datasets.
        The min_trials filter isn't implemented yet (would require aggregation).

    Example:
        >>> sponsors = load_sponsors(limit=100)
        >>> print(f"Loaded {len(sponsors)} sponsors for analysis")
    """
    catalog = _get_catalog()
    table = catalog.load_table("clinical.trials")

    # Scan for sponsor names
    scan = table.scan(
        selected_fields=["sponsor_name"],
        row_filter="sponsor_name IS NOT NULL",
    )

    # Convert to Polars and get unique values
    arrow_table = scan.to_arrow()
    df = pl.from_arrow(arrow_table).unique().sort("sponsor_name")

    if limit:
        df = df.head(limit)

    print(f"Loaded {len(df)} unique sponsors from warehouse")
    if limit:
        print(f"  (limited to {limit} for development)")

    return df


def load_tickers(
    limit: int | None = None,
    market_filter: str | None = None,
) -> pl.DataFrame:
    """Load ticker/company data from market warehouse.

    Queries the market.ticker_details table for public company information.
    Used to build the target entities for sponsor matching.

    Args:
        limit: Maximum number of tickers (for development/testing)
        market_filter: Filter by market type, e.g. "stocks" (default: all)

    Returns:
        DataFrame with columns:
        - ticker: str - Stock symbol
        - name: str - Company name
        - market: str - Market type (stocks, etf, etc.)
        - description: str - Company description (may be null)
        - industry: str - Industry classification (may be null)
        - sector: str - Sector classification (may be null)

    Example:
        >>> tickers = load_tickers(market_filter="stocks", limit=500)
        >>> print(f"Loaded {len(tickers)} stock tickers")
    """
    catalog = _get_catalog()
    table = catalog.load_table("market.ticker_details")

    # Build filter
    row_filter = "name IS NOT NULL"
    if market_filter:
        row_filter += f" AND market = '{market_filter}'"

    # Scan for ticker details (now includes industry and sector)
    scan = table.scan(
        selected_fields=["ticker", "name", "market", "description", "industry", "sector"],
        row_filter=row_filter,
    )

    # Convert to Polars
    arrow_table = scan.to_arrow()
    df = pl.from_arrow(arrow_table).sort("name")

    if limit:
        df = df.head(limit)

    print(f"Loaded {len(df)} tickers from warehouse")
    if market_filter:
        print(f"  (filtered to market={market_filter})")
    if limit:
        print(f"  (limited to {limit} for development)")

    return df


def get_warehouse_info() -> dict:
    """Get information about the configured S3 Tables warehouse.

    Returns:
        Dictionary with:
        - bucket_arn: S3 Tables bucket ARN
        - region: AWS region
        - catalog_type: Catalog implementation (s3tables)
    """
    bucket_arn = os.environ.get(
        "TABLE_BUCKET_ARN",
        "arn:aws:s3tables:us-east-1:620117234001:bucket/petals-tables-620117234001",
    )
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    return {
        "bucket_arn": bucket_arn,
        "region": region,
        "catalog_type": "s3tables",
    }
