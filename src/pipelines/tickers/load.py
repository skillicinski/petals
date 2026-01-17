"""
Load ticker data to S3 Tables (Iceberg).
"""

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, BooleanType, NestedField


# Schema for ticker reference data
TICKER_SCHEMA = Schema(
    NestedField(1, "ticker", StringType(), required=True),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "market", StringType(), required=False),
    NestedField(4, "locale", StringType(), required=False),
    NestedField(5, "primary_exchange", StringType(), required=False),
    NestedField(6, "type", StringType(), required=False),
    NestedField(7, "active", BooleanType(), required=False),
    NestedField(8, "currency_name", StringType(), required=False),
    NestedField(9, "cik", StringType(), required=False),
    NestedField(10, "composite_figi", StringType(), required=False),
)


def get_catalog(table_bucket_arn: str, region: str = "us-east-1"):
    """
    Get PyIceberg catalog connected to S3 Tables REST endpoint.
    
    Args:
        table_bucket_arn: ARN of the S3 Table Bucket
        region: AWS region
    """
    return load_catalog(
        "s3tables",
        type="rest",
        uri=f"https://s3tables.{region}.amazonaws.com/iceberg",
        warehouse=table_bucket_arn,
        **{
            "rest.sigv4-enabled": "true",
            "rest.signing-region": region,
            "rest.signing-name": "s3tables",
        },
    )


def ensure_namespace(catalog, namespace: str) -> None:
    """Create namespace if it doesn't exist."""
    try:
        catalog.create_namespace(namespace)
        print(f"Created namespace: {namespace}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"Namespace exists: {namespace}")
        else:
            raise


def tickers_to_arrow(tickers: list[dict]) -> pa.Table:
    """Convert ticker dicts to PyArrow table."""
    return pa.Table.from_pylist(
        [
            {
                "ticker": t.get("ticker"),
                "name": t.get("name"),
                "market": t.get("market"),
                "locale": t.get("locale"),
                "primary_exchange": t.get("primary_exchange"),
                "type": t.get("type"),
                "active": t.get("active"),
                "currency_name": t.get("currency_name"),
                "cik": t.get("cik"),
                "composite_figi": t.get("composite_figi"),
            }
            for t in tickers
        ],
        schema=pa.schema(
            [
                pa.field("ticker", pa.string(), nullable=False),
                pa.field("name", pa.string(), nullable=False),
                pa.field("market", pa.string(), nullable=True),
                pa.field("locale", pa.string(), nullable=True),
                pa.field("primary_exchange", pa.string(), nullable=True),
                pa.field("type", pa.string(), nullable=True),
                pa.field("active", pa.bool_(), nullable=True),
                pa.field("currency_name", pa.string(), nullable=True),
                pa.field("cik", pa.string(), nullable=True),
                pa.field("composite_figi", pa.string(), nullable=True),
            ]
        ),
    )


def load_tickers(
    tickers: list[dict],
    table_bucket_arn: str,
    namespace: str = "reference",
    table_name: str = "tickers",
    region: str = "us-east-1",
) -> None:
    """
    Load tickers to S3 Tables Iceberg table.
    
    Creates namespace and table if they don't exist.
    Overwrites existing data (full refresh).
    """
    catalog = get_catalog(table_bucket_arn, region)
    
    # Ensure namespace exists
    ensure_namespace(catalog, namespace)
    
    table_id = f"{namespace}.{table_name}"
    
    # Create or replace table
    arrow_table = tickers_to_arrow(tickers)
    
    try:
        table = catalog.load_table(table_id)
        print(f"Table exists: {table_id}, overwriting...")
        table.overwrite(arrow_table)
    except NoSuchTableError:
        print(f"Creating table: {table_id}")
        table = catalog.create_table(table_id, schema=TICKER_SCHEMA)
        table.overwrite(arrow_table)
    
    print(f"Loaded {len(tickers)} tickers to {table_id}")
