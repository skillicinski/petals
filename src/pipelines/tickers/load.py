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
    NestedField(1, "ticker", StringType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "market", StringType(), required=True),
    NestedField(4, "locale", StringType(), required=False),
    NestedField(5, "primary_exchange", StringType(), required=False),
    NestedField(6, "type", StringType(), required=False),
    NestedField(7, "active", BooleanType(), required=False),
    NestedField(8, "currency_name", StringType(), required=False),
    NestedField(9, "cik", StringType(), required=False),
    NestedField(10, "composite_figi", StringType(), required=False),
    NestedField(11, "delisted_utc", StringType(), required=False),
    NestedField(12, "last_updated_utc", StringType(), required=False),
    identifier_field_ids=[1, 3],
)

# Arrow schema matching the Iceberg schema
TICKER_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("market", pa.string(), nullable=False),
        pa.field("locale", pa.string(), nullable=True),
        pa.field("primary_exchange", pa.string(), nullable=True),
        pa.field("type", pa.string(), nullable=True),
        pa.field("active", pa.bool_(), nullable=True),
        pa.field("currency_name", pa.string(), nullable=True),
        pa.field("cik", pa.string(), nullable=True),
        pa.field("composite_figi", pa.string(), nullable=True),
        pa.field("delisted_utc", pa.string(), nullable=True),
        pa.field("last_updated_utc", pa.string(), nullable=True),
    ]
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
    """
    Convert ticker dicts to PyArrow table.

    Deduplicates by (ticker, market) composite key since the API
    occasionally returns duplicates within the same market.
    """
    # Dedupe by (ticker, market) - last occurrence wins
    seen = {}
    for t in tickers:
        ticker = t.get("ticker")
        market = t.get("market")
        if ticker and market:
            key = (ticker, market)
            seen[key] = {
                "ticker": ticker,
                "name": t.get("name"),
                "market": market,
                "locale": t.get("locale"),
                "primary_exchange": t.get("primary_exchange"),
                "type": t.get("type"),
                "active": t.get("active"),
                "currency_name": t.get("currency_name"),
                "cik": t.get("cik"),
                "composite_figi": t.get("composite_figi"),
                "delisted_utc": t.get("delisted_utc"),
                "last_updated_utc": t.get("last_updated_utc"),
            }

    return pa.Table.from_pylist(list(seen.values()), schema=TICKER_ARROW_SCHEMA)


def table_exists(
    table_bucket_arn: str,
    namespace: str = "reference",
    table_name: str = "tickers",
    region: str = "us-east-1",
) -> bool:
    """Check if the tickers table exists."""
    try:
        catalog = get_catalog(table_bucket_arn, region)
        catalog.load_table(f"{namespace}.{table_name}")
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


def log(msg: str) -> None:
    """Print and flush immediately to ensure logs are captured before crashes."""
    import sys

    print(msg)
    sys.stdout.flush()


def load_tickers(
    tickers: list[dict],
    table_bucket_arn: str,
    namespace: str = "reference",
    table_name: str = "tickers",
    region: str = "us-east-1",
    batch_size: int = 5000,
) -> dict:
    """
    Load tickers to S3 Tables Iceberg table using upsert.

    Creates namespace and table if they don't exist.
    Uses upsert to only write changed rows (matched by ticker, market).
    Processes in batches to isolate failures and reduce memory pressure.

    Args:
        tickers: List of ticker dicts from Massive API
        table_bucket_arn: ARN of the S3 Table Bucket
        namespace: Iceberg namespace (default: reference)
        table_name: Table name (default: tickers)
        region: AWS region
        batch_size: Number of rows per upsert batch (default: 5000)

    Returns:
        Dict with rows_inserted and rows_updated counts
    """
    import os
    import traceback

    log(f"[load] Starting load_tickers with {len(tickers)} tickers")
    log(f"[load] batch_size={batch_size}, region={region}")

    # Allow override via env var for testing
    batch_size = int(os.environ.get("UPSERT_BATCH_SIZE", batch_size))
    log(f"[load] Effective batch_size={batch_size}")

    log("[load] Getting catalog...")
    catalog = get_catalog(table_bucket_arn, region)
    log("[load] Catalog obtained")

    # Ensure namespace exists
    ensure_namespace(catalog, namespace)

    table_id = f"{namespace}.{table_name}"

    # Convert to Arrow (deduplicates by ticker, market)
    log("[load] Converting to Arrow table...")
    arrow_table = tickers_to_arrow(tickers)
    deduped_count = len(arrow_table)
    if deduped_count != len(tickers):
        dupes = len(tickers) - deduped_count
        log(f"[load] Incoming: {len(tickers)} tickers ({dupes} duplicates removed)")
    else:
        log(f"[load] Incoming: {deduped_count} tickers")

    log(f"[load] Arrow table memory: {arrow_table.nbytes / 1024 / 1024:.2f} MB")

    total_inserted = 0
    total_updated = 0

    try:
        table = catalog.load_table(table_id)
        log(f"[load] Table exists: {table_id}")
        log(f"[load] Table schema: {table.schema()}")

        # Process in batches
        num_batches = (deduped_count + batch_size - 1) // batch_size
        log(f"[load] Will process {num_batches} batches of up to {batch_size} rows")

        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, deduped_count)
            batch = arrow_table.slice(start_idx, end_idx - start_idx)

            log(
                f"[load] Batch {i + 1}/{num_batches}: "
                f"rows {start_idx}-{end_idx} ({len(batch)} rows)"
            )
            log(f"[load] Batch memory: {batch.nbytes / 1024 / 1024:.2f} MB")

            try:
                log(f"[load] Starting upsert for batch {i + 1}...")
                result = table.upsert(batch, join_cols=["ticker", "market"])
                log(
                    f"[load] Batch {i + 1} complete: "
                    f"{result.rows_inserted} inserted, {result.rows_updated} updated"
                )
                total_inserted += result.rows_inserted
                total_updated += result.rows_updated
            except Exception as e:
                log(f"[load] ERROR in batch {i + 1}: {type(e).__name__}: {e}")
                log(f"[load] Traceback:\n{traceback.format_exc()}")
                raise

        log(f"[load] All batches complete: {total_inserted} inserted, {total_updated} updated")
        return {"rows_inserted": total_inserted, "rows_updated": total_updated}

    except NoSuchTableError:
        log(f"[load] Creating table: {table_id}")
        table = catalog.create_table(table_id, schema=TICKER_SCHEMA)
        log("[load] Table created, appending data...")
        table.append(arrow_table)
        log(f"[load] Initial load complete: {deduped_count} tickers")
        return {"rows_inserted": deduped_count, "rows_updated": 0}
    except Exception as e:
        log(f"[load] FATAL ERROR: {type(e).__name__}: {e}")
        log(f"[load] Traceback:\n{traceback.format_exc()}")
        raise
