"""
Load ticker details to S3 Tables (Iceberg).

Uses SCD Type 1 (update in place) with PyIceberg's native upsert.
The (ticker, market) composite key identifies rows for upsert matching.
"""

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, IntegerType, NestedField, StringType

# Schema for ticker details
# (ticker, market) composite key matches the tickers table
TICKER_DETAILS_SCHEMA = Schema(
    NestedField(1, "ticker", StringType(), required=True),
    NestedField(2, "market", StringType(), required=True),
    NestedField(3, "name", StringType(), required=False),
    NestedField(4, "description", StringType(), required=False),
    NestedField(5, "homepage_url", StringType(), required=False),
    NestedField(6, "market_cap", DoubleType(), required=False),
    NestedField(7, "total_employees", IntegerType(), required=False),
    NestedField(8, "locale", StringType(), required=False),
    NestedField(9, "primary_exchange", StringType(), required=False),
    NestedField(10, "currency_name", StringType(), required=False),
    NestedField(11, "address_city", StringType(), required=False),
    NestedField(12, "address_state", StringType(), required=False),
    NestedField(13, "address_country", StringType(), required=False),
    NestedField(14, "industry", StringType(), required=False),
    NestedField(15, "sector", StringType(), required=False),
    NestedField(16, "last_fetched_utc", StringType(), required=False),
    identifier_field_ids=[1, 2],
)

# Arrow schema matching the Iceberg schema
TICKER_DETAILS_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("market", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("description", pa.string(), nullable=True),
        pa.field("homepage_url", pa.string(), nullable=True),
        pa.field("market_cap", pa.float64(), nullable=True),
        pa.field("total_employees", pa.int32(), nullable=True),
        pa.field("locale", pa.string(), nullable=True),
        pa.field("primary_exchange", pa.string(), nullable=True),
        pa.field("currency_name", pa.string(), nullable=True),
        pa.field("address_city", pa.string(), nullable=True),
        pa.field("address_state", pa.string(), nullable=True),
        pa.field("address_country", pa.string(), nullable=True),
        pa.field("industry", pa.string(), nullable=True),
        pa.field("sector", pa.string(), nullable=True),
        pa.field("last_fetched_utc", pa.string(), nullable=True),
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


def details_to_arrow(details: list[dict]) -> pa.Table:
    """
    Convert ticker detail dicts to PyArrow table.

    Deduplicates by (ticker, market) composite key.
    """
    seen = {}
    for d in details:
        ticker = d.get("ticker")
        market = d.get("market")
        if ticker and market:
            key = (ticker, market)
            seen[key] = d

    return pa.Table.from_pylist(list(seen.values()), schema=TICKER_DETAILS_ARROW_SCHEMA)


def table_exists(
    table_bucket_arn: str,
    namespace: str = "market",
    table_name: str = "ticker_details",
    region: str = "us-east-1",
) -> bool:
    """Check if the ticker_details table exists."""
    try:
        catalog = get_catalog(table_bucket_arn, region)
        catalog.load_table(f"{namespace}.{table_name}")
        return True
    except NoSuchTableError:
        return False


def log(msg: str) -> None:
    """Print and flush immediately."""
    import sys

    print(msg)
    sys.stdout.flush()


def log_memory(label: str) -> None:
    """Log current memory usage with a label."""
    import psutil

    proc = psutil.Process()
    mem = proc.memory_info()
    rss_mb = mem.rss / 1024 / 1024
    log(f"[memory] {label}: {rss_mb:.1f} MB RSS")


def load_ticker_details(
    details: list[dict],
    table_bucket_arn: str,
    namespace: str = "market",
    table_name: str = "ticker_details",
    region: str = "us-east-1",
    batch_size: int = 1000,
) -> dict:
    """
    Load ticker details to S3 Tables Iceberg table using upsert.

    Creates namespace and table if they don't exist.
    Uses PyIceberg's upsert which automatically detects unchanged rows.

    Args:
        details: List of ticker detail dicts
        table_bucket_arn: ARN of the S3 Table Bucket
        namespace: Iceberg namespace (default: reference)
        table_name: Table name (default: ticker_details)
        region: AWS region
        batch_size: Number of rows per upsert batch

    Returns:
        Dict with rows_inserted and rows_updated counts
    """
    import os
    import traceback

    log(f"[load] Starting load_ticker_details with {len(details)} records")

    batch_size = int(os.environ.get("UPSERT_BATCH_SIZE", batch_size))
    log(f"[load] batch_size={batch_size}")

    log("[load] Getting catalog...")
    catalog = get_catalog(table_bucket_arn, region)
    log("[load] Catalog obtained")

    ensure_namespace(catalog, namespace)

    table_id = f"{namespace}.{table_name}"

    log("[load] Converting to Arrow table...")
    arrow_table = details_to_arrow(details)
    deduped_count = len(arrow_table)

    if deduped_count != len(details):
        dupes = len(details) - deduped_count
        log(f"[load] Incoming: {len(details)} records ({dupes} duplicates removed)")
    else:
        log(f"[load] Incoming: {deduped_count} records")

    log(f"[load] Arrow table memory: {arrow_table.nbytes / 1024 / 1024:.2f} MB")
    log_memory("post-arrow-convert")

    total_inserted = 0
    total_updated = 0

    try:
        table = catalog.load_table(table_id)
        log(f"[load] Table exists: {table_id}")

        num_batches = (deduped_count + batch_size - 1) // batch_size
        log(f"[load] Will process {num_batches} batches of up to {batch_size} rows")

        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, deduped_count)
            batch = arrow_table.slice(start_idx, end_idx - start_idx)

            batch_info = f"rows {start_idx}-{end_idx} ({len(batch)} rows)"
            log(f"[load] Batch {i + 1}/{num_batches}: {batch_info}")
            log_memory(f"batch-{i + 1}-start")

            try:
                log(f"[load] Starting upsert for batch {i + 1}...")
                result = table.upsert(batch, join_cols=["ticker", "market"])
                inserted, updated = result.rows_inserted, result.rows_updated
                log(f"[load] Batch {i + 1} complete: {inserted} inserted, {updated} updated")
                total_inserted += result.rows_inserted
                total_updated += result.rows_updated
            except Exception as e:
                log(f"[load] ERROR in batch {i + 1}: {type(e).__name__}: {e}")
                log(f"[load] Traceback:\n{traceback.format_exc()}")
                raise

        log(f"[load] All batches complete: {total_inserted} inserted, {total_updated} updated")
        log_memory("complete")
        return {"rows_inserted": total_inserted, "rows_updated": total_updated}

    except NoSuchTableError:
        log(f"[load] Creating table: {table_id}")
        table = catalog.create_table(table_id, schema=TICKER_DETAILS_SCHEMA)
        log("[load] Table created, appending data...")
        table.append(arrow_table)
        log(f"[load] Initial load complete: {deduped_count} records")
        log_memory("complete")
        return {"rows_inserted": deduped_count, "rows_updated": 0}

    except Exception as e:
        log(f"[load] FATAL ERROR: {type(e).__name__}: {e}")
        log(f"[load] Traceback:\n{traceback.format_exc()}")
        raise
