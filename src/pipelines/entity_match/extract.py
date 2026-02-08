"""Extract entities from S3 Tables for matching.

Fetches:
- Left side: distinct sponsors from clinical.trials
- Right side: companies from market.ticker_details
"""

import os

import polars as pl
from pyiceberg.catalog import load_catalog


def get_catalog():
    """Load PyIceberg catalog for S3 Tables."""
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


def fetch_sponsors(limit: int | None = None) -> pl.DataFrame:
    """Fetch distinct sponsor names from clinical.trials.

    Returns DataFrame with columns: sponsor_name
    """
    catalog = get_catalog()
    table = catalog.load_table("clinical.trials")

    scan = table.scan(selected_fields=["sponsor_name"], row_filter="sponsor_name IS NOT NULL")
    arrow_table = scan.to_arrow()

    df = pl.from_arrow(arrow_table).unique().sort("sponsor_name")

    if limit:
        df = df.head(limit)

    print(f"[extract] Fetched {len(df)} distinct sponsors from clinical.trials")
    return df


def fetch_tickers(limit: int | None = None) -> pl.DataFrame:
    """Fetch companies from ticker_details table.

    Returns DataFrame with columns: ticker, market, name, description
    """
    catalog = get_catalog()
    table = catalog.load_table("market.ticker_details")

    scan = table.scan(
        selected_fields=["ticker", "market", "name", "description"], row_filter="name IS NOT NULL"
    )
    arrow_table = scan.to_arrow()

    df = pl.from_arrow(arrow_table).sort("name")

    if limit:
        df = df.head(limit)

    print(f"[extract] Fetched {len(df)} tickers from ticker_details")
    return df
