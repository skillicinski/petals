"""
Load OHLC price data to S3 Tables (Iceberg).

Uses SCD Type 1 (update in place) with PyIceberg's native upsert.
The (ticker, date) composite key identifies rows for upsert matching.
"""

import sys

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

# Schema for ticker prices (OHLC)
# (ticker, date) composite key
TICKER_PRICES_SCHEMA = Schema(
    NestedField(1, "ticker", StringType(), required=True),
    NestedField(2, "date", StringType(), required=True),
    NestedField(3, "open", DoubleType(), required=False),
    NestedField(4, "high", DoubleType(), required=False),
    NestedField(5, "low", DoubleType(), required=False),
    NestedField(6, "close", DoubleType(), required=False),
    NestedField(7, "volume", LongType(), required=False),
    NestedField(8, "last_fetched_utc", StringType(), required=False),
    NestedField(9, "market", StringType(), required=False),
    NestedField(10, "locale", StringType(), required=False),
    identifier_field_ids=[1, 2],
)

# Arrow schema matching the Iceberg schema
TICKER_PRICES_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("date", pa.string(), nullable=False),
        pa.field("open", pa.float64(), nullable=True),
        pa.field("high", pa.float64(), nullable=True),
        pa.field("low", pa.float64(), nullable=True),
        pa.field("close", pa.float64(), nullable=True),
        pa.field("volume", pa.int64(), nullable=True),
        pa.field("last_fetched_utc", pa.string(), nullable=True),
        pa.field("market", pa.string(), nullable=True),
        pa.field("locale", pa.string(), nullable=True),
    ]
)


def log(msg: str) -> None:
    """Print and flush immediately."""
    print(msg)
    sys.stdout.flush()


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
        log(f"[load] Created namespace: {namespace}")
    except Exception as e:
        if "already exists" in str(e).lower():
            log(f"[load] Namespace exists: {namespace}")
        else:
            raise


def evolve_schema(table, target_schema: Schema) -> None:
    """
    Evolve table schema to match target schema by adding missing columns.
    
    Uses PyIceberg's schema evolution API to add columns that exist in the
    target schema but not in the current table schema.
    
    Args:
        table: PyIceberg table instance
        target_schema: Target schema with desired columns
        
    Raises:
        Exception: If schema evolution fails, logged and re-raised for troubleshooting
    """
    import traceback
    
    current_schema = table.schema()
    current_fields = {field.name for field in current_schema.fields}
    target_fields = {field.name: field for field in target_schema.fields}
    
    missing_fields = set(target_fields.keys()) - current_fields
    
    if not missing_fields:
        log("[load] Schema is up to date, no evolution needed")
        return
    
    log(f"[load] Schema evolution required: adding {len(missing_fields)} column(s): {sorted(missing_fields)}")
    
    try:
        with table.update_schema() as update:
            for field_name in sorted(missing_fields):
                field = target_fields[field_name]
                log(f"[load] Adding column: {field_name} ({field.field_type}, required={field.required})")
                update.add_column(
                    field_name,
                    field.field_type,
                    doc=field.doc if hasattr(field, 'doc') else None,
                    required=field.required
                )
        
        log("[load] Schema evolution successful")
        
    except Exception as e:
        log(f"[load] SCHEMA EVOLUTION FAILED: {type(e).__name__}: {e}")
        log(f"[load] Failed to add columns: {sorted(missing_fields)}")
        log(f"[load] Traceback:\n{traceback.format_exc()}")
        raise Exception(
            f"Schema evolution failed while adding columns {sorted(missing_fields)}: {e}"
        ) from e


def prices_to_arrow(prices: list[dict]) -> pa.Table:
    """
    Convert price dicts to PyArrow table.

    Deduplicates by (ticker, date) composite key.
    """
    seen = {}
    for p in prices:
        ticker = p.get("ticker")
        date = p.get("date")
        if ticker and date:
            key = (ticker, date)
            seen[key] = p

    return pa.Table.from_pylist(list(seen.values()), schema=TICKER_PRICES_ARROW_SCHEMA)


def table_exists(
    table_bucket_arn: str,
    namespace: str = "market_data",
    table_name: str = "ticker_prices",
    region: str = "us-east-1",
) -> bool:
    """Check if the ticker_prices table exists."""
    try:
        catalog = get_catalog(table_bucket_arn, region)
        catalog.load_table(f"{namespace}.{table_name}")
        return True
    except NoSuchTableError:
        return False


def load_ticker_prices(
    prices: list[dict],
    table_bucket_arn: str,
    namespace: str = "market_data",
    table_name: str = "ticker_prices",
    region: str = "us-east-1",
    batch_size: int = 1000,
) -> dict:
    """
    Load ticker prices to S3 Tables Iceberg table using upsert.

    Creates namespace and table if they don't exist.
    Uses PyIceberg's upsert which automatically detects unchanged rows.

    Args:
        prices: List of price dicts
        table_bucket_arn: ARN of the S3 Table Bucket
        namespace: Iceberg namespace (default: market_data)
        table_name: Table name (default: ticker_prices)
        region: AWS region
        batch_size: Number of rows per upsert batch

    Returns:
        Dict with rows_inserted and rows_updated counts
    """
    import os
    import traceback

    log(f"[load] Starting load_ticker_prices with {len(prices):,} records")

    batch_size = int(os.environ.get("UPSERT_BATCH_SIZE", batch_size))
    log(f"[load] batch_size={batch_size}")

    log("[load] Getting catalog...")
    catalog = get_catalog(table_bucket_arn, region)
    log("[load] Catalog obtained")

    ensure_namespace(catalog, namespace)

    table_id = f"{namespace}.{table_name}"

    log("[load] Converting to Arrow table...")
    arrow_table = prices_to_arrow(prices)
    deduped_count = len(arrow_table)

    if deduped_count != len(prices):
        dupes = len(prices) - deduped_count
        log(f"[load] Incoming: {len(prices):,} records ({dupes} duplicates removed)")
    else:
        log(f"[load] Incoming: {deduped_count:,} records")

    log(f"[load] Arrow table memory: {arrow_table.nbytes / 1024 / 1024:.2f} MB")

    total_inserted = 0
    total_updated = 0

    try:
        table = catalog.load_table(table_id)
        log(f"[load] Table exists: {table_id}")
        
        # Evolve schema if needed (add missing columns)
        evolve_schema(table, TICKER_PRICES_SCHEMA)

        num_batches = (deduped_count + batch_size - 1) // batch_size
        log(f"[load] Will process {num_batches} batches of up to {batch_size} rows")

        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, deduped_count)
            batch = arrow_table.slice(start_idx, end_idx - start_idx)

            batch_info = f"rows {start_idx}-{end_idx} ({len(batch)} rows)"
            log(f"[load] Batch {i + 1}/{num_batches}: {batch_info}")

            try:
                log(f"[load] Starting upsert for batch {i + 1}...")
                result = table.upsert(batch, join_cols=["ticker", "date"])
                inserted, updated = result.rows_inserted, result.rows_updated
                log(f"[load] Batch {i + 1} complete: {inserted} inserted, {updated} updated")
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
        table = catalog.create_table(table_id, schema=TICKER_PRICES_SCHEMA)
        log("[load] Table created, appending data...")
        table.append(arrow_table)
        log(f"[load] Initial load complete: {deduped_count:,} records")
        return {"rows_inserted": deduped_count, "rows_updated": 0}

    except Exception as e:
        log(f"[load] FATAL ERROR: {type(e).__name__}: {e}")
        log(f"[load] Traceback:\n{traceback.format_exc()}")
        raise
