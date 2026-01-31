"""Extract company data from S3 Tables for alias generation."""

import os

import polars as pl
from pyiceberg.catalog import load_catalog


def get_catalog():
    """Load PyIceberg catalog for S3 Tables."""
    bucket_arn = os.environ.get(
        'TABLE_BUCKET_ARN',
        'arn:aws:s3tables:us-east-1:620117234001:bucket/petals-tables-620117234001'
    )
    region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    return load_catalog(
        's3tables',
        type='rest',
        uri=f'https://s3tables.{region}.amazonaws.com/iceberg',
        warehouse=bucket_arn,
        **{
            'rest.sigv4-enabled': 'true',
            'rest.signing-region': region,
            'rest.signing-name': 's3tables',
        }
    )


def fetch_companies(limit: int | None = None) -> pl.DataFrame:
    """Fetch companies from ticker_details table.

    Returns DataFrame with columns: ticker, market, name, description
    """
    catalog = get_catalog()
    table = catalog.load_table('reference.ticker_details')

    # Read to Arrow then Polars
    scan = table.scan(
        selected_fields=['ticker', 'market', 'name', 'description'],
        row_filter='description IS NOT NULL'
    )
    arrow_table = scan.to_arrow()

    df = pl.from_arrow(arrow_table)

    if limit:
        df = df.head(limit)

    print(f'[extract] Fetched {len(df)} companies from ticker_details')
    return df


def fetch_existing_aliases() -> dict[str, list[str]]:
    """Fetch existing aliases from alias table (if exists).

    Returns dict mapping (ticker, market) -> list of aliases.
    """
    try:
        catalog = get_catalog()
        table = catalog.load_table('reference.ticker_aliases')
        arrow_table = table.scan().to_arrow()
        df = pl.from_arrow(arrow_table)

        # Build lookup dict
        aliases = {}
        for row in df.iter_rows(named=True):
            key = f"{row['ticker']}|{row['market']}"
            aliases[key] = row.get('aliases', [])

        print(f'[extract] Loaded {len(aliases)} existing alias records')
        return aliases
    except Exception:
        print('[extract] No existing alias table found')
        return {}
