"""
Load ticker data to S3 Tables (Iceberg).

Uses SCD Type 1 (update in place) with PyIceberg's native upsert.
The (ticker, market) composite key identifies rows for upsert matching.
"""

from datetime import datetime, timedelta

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, NestedField, StringType

# Schema for ticker reference data
# identifier_field_ids marks (ticker, market) as the composite primary key
# Same ticker can exist in different markets (e.g., BITW in stocks and otc)
TICKER_SCHEMA = Schema(
    NestedField(1, 'ticker', StringType(), required=True),
    NestedField(2, 'name', StringType(), required=False),
    NestedField(3, 'market', StringType(), required=True),
    NestedField(4, 'locale', StringType(), required=False),
    NestedField(5, 'primary_exchange', StringType(), required=False),
    NestedField(6, 'type', StringType(), required=False),
    NestedField(7, 'active', BooleanType(), required=False),
    NestedField(8, 'currency_name', StringType(), required=False),
    NestedField(9, 'cik', StringType(), required=False),
    NestedField(10, 'composite_figi', StringType(), required=False),
    NestedField(11, 'delisted_utc', StringType(), required=False),
    NestedField(12, 'last_updated_utc', StringType(), required=False),
    identifier_field_ids=[1, 3],
)

# Arrow schema matching the Iceberg schema
TICKER_ARROW_SCHEMA = pa.schema([
    pa.field('ticker', pa.string(), nullable=False),
    pa.field('name', pa.string(), nullable=True),
    pa.field('market', pa.string(), nullable=False),
    pa.field('locale', pa.string(), nullable=True),
    pa.field('primary_exchange', pa.string(), nullable=True),
    pa.field('type', pa.string(), nullable=True),
    pa.field('active', pa.bool_(), nullable=True),
    pa.field('currency_name', pa.string(), nullable=True),
    pa.field('cik', pa.string(), nullable=True),
    pa.field('composite_figi', pa.string(), nullable=True),
    pa.field('delisted_utc', pa.string(), nullable=True),
    pa.field('last_updated_utc', pa.string(), nullable=True),
])


def get_catalog(table_bucket_arn: str, region: str = 'us-east-1'):
    """
    Get PyIceberg catalog connected to S3 Tables REST endpoint.

    Args:
        table_bucket_arn: ARN of the S3 Table Bucket
        region: AWS region
    """
    return load_catalog(
        's3tables',
        type='rest',
        uri=f'https://s3tables.{region}.amazonaws.com/iceberg',
        warehouse=table_bucket_arn,
        **{
            'rest.sigv4-enabled': 'true',
            'rest.signing-region': region,
            'rest.signing-name': 's3tables',
        },
    )


def ensure_namespace(catalog, namespace: str) -> None:
    """Create namespace if it doesn't exist."""
    try:
        catalog.create_namespace(namespace)
        print(f'Created namespace: {namespace}')
    except Exception as e:
        if 'already exists' in str(e).lower():
            print(f'Namespace exists: {namespace}')
        else:
            raise


def tickers_to_arrow(tickers: list[dict]) -> pa.Table:
    """
    Convert ticker dicts to PyArrow table.

    Deduplicates by (ticker, market) composite key since the API
    occasionally returns duplicates within the same market.
    """
    # Dedupe by (ticker, market) - last occurrence wins
    seen = {}
    for t in tickers:
        ticker = t.get('ticker')
        market = t.get('market')
        if ticker and market:
            key = (ticker, market)
            seen[key] = {
                'ticker': ticker,
                'name': t.get('name'),
                'market': market,
                'locale': t.get('locale'),
                'primary_exchange': t.get('primary_exchange'),
                'type': t.get('type'),
                'active': t.get('active'),
                'currency_name': t.get('currency_name'),
                'cik': t.get('cik'),
                'composite_figi': t.get('composite_figi'),
                'delisted_utc': t.get('delisted_utc'),
                'last_updated_utc': t.get('last_updated_utc'),
            }

    return pa.Table.from_pylist(list(seen.values()), schema=TICKER_ARROW_SCHEMA)


def table_exists(
    table_bucket_arn: str,
    namespace: str = 'reference',
    table_name: str = 'tickers',
    region: str = 'us-east-1',
) -> bool:
    """Check if the tickers table exists."""
    try:
        catalog = get_catalog(table_bucket_arn, region)
        catalog.load_table(f'{namespace}.{table_name}')
        return True
    except NoSuchTableError:
        return False


def get_incremental_cutoff(hours_ago: int = 24) -> datetime:
    """
    Get cutoff timestamp for incremental fetching.

    Uses current time minus a buffer. This is more reliable than using
    max(last_updated_utc) from the table, because that timestamp reflects
    when Massive last updated the ticker, not when we last ran the pipeline.

    For proper tracking, this should eventually be replaced with a state store
    (DynamoDB, Step Functions, etc.) that records actual pipeline run times.

    Args:
        hours_ago: How many hours back to look for updates (default: 24)
    """
    return datetime.utcnow() - timedelta(hours=hours_ago)


def load_tickers(
    tickers: list[dict],
    table_bucket_arn: str,
    namespace: str = 'reference',
    table_name: str = 'tickers',
    region: str = 'us-east-1',
) -> dict:
    """
    Load tickers to S3 Tables Iceberg table using upsert.

    Creates namespace and table if they don't exist.
    Uses upsert to only write changed rows (matched by ticker).

    Args:
        tickers: List of ticker dicts from Massive API
        table_bucket_arn: ARN of the S3 Table Bucket
        namespace: Iceberg namespace (default: reference)
        table_name: Table name (default: tickers)
        region: AWS region

    Returns:
        Dict with rows_inserted and rows_updated counts
    """
    catalog = get_catalog(table_bucket_arn, region)

    # Ensure namespace exists
    ensure_namespace(catalog, namespace)

    table_id = f'{namespace}.{table_name}'

    # Convert to Arrow (deduplicates by ticker)
    arrow_table = tickers_to_arrow(tickers)
    deduped_count = len(arrow_table)
    if deduped_count != len(tickers):
        print(f'Incoming: {len(tickers)} tickers ({len(tickers) - deduped_count} duplicates removed)')
    else:
        print(f'Incoming: {deduped_count} tickers')

    try:
        table = catalog.load_table(table_id)
        print(f'Table exists: {table_id}, upserting...')

        result = table.upsert(arrow_table)
        print(
            f'Upsert complete: {result.rows_inserted} inserted, '
            f'{result.rows_updated} updated'
        )
        return {'rows_inserted': result.rows_inserted, 'rows_updated': result.rows_updated}

    except NoSuchTableError:
        print(f'Creating table: {table_id}')
        table = catalog.create_table(table_id, schema=TICKER_SCHEMA)
        table.append(arrow_table)
        print(f'Initial load: {len(tickers)} tickers')
        return {'rows_inserted': len(tickers), 'rows_updated': 0}
