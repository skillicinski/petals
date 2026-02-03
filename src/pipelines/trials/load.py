"""
Load clinical trial data to S3 Tables (Iceberg).

Uses SCD Type 1 (update in place) with PyIceberg's native upsert.
The nct_id is the primary key for upsert matching.
"""

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, IntegerType, NestedField, StringType

# Schema for clinical trial data
# nct_id is the primary key (identifier_field_ids)
STUDY_SCHEMA = Schema(
    NestedField(1, "nct_id", StringType(), required=True),
    NestedField(2, "title", StringType(), required=False),
    NestedField(3, "official_title", StringType(), required=False),
    NestedField(4, "sponsor_name", StringType(), required=False),
    NestedField(5, "sponsor_class", StringType(), required=False),
    NestedField(6, "organization_name", StringType(), required=False),
    NestedField(7, "organization_class", StringType(), required=False),
    NestedField(8, "overall_status", StringType(), required=False),
    NestedField(9, "completion_date", StringType(), required=False),
    NestedField(10, "completion_date_type", StringType(), required=False),
    NestedField(11, "primary_completion_date", StringType(), required=False),
    NestedField(12, "study_type", StringType(), required=False),
    NestedField(20, "phases", StringType(), required=False),  # JSON array
    NestedField(13, "enrollment_count", IntegerType(), required=False),
    NestedField(14, "enrollment_type", StringType(), required=False),
    NestedField(15, "has_results", BooleanType(), required=False),
    NestedField(16, "conditions", StringType(), required=False),  # JSON array
    NestedField(17, "interventions", StringType(), required=False),  # JSON array
    NestedField(18, "last_update_date", StringType(), required=False),
    NestedField(19, "study_first_submit_date", StringType(), required=False),
    identifier_field_ids=[1],
)

# Arrow schema matching the Iceberg schema
STUDY_ARROW_SCHEMA = pa.schema(
    [
        pa.field("nct_id", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=True),
        pa.field("official_title", pa.string(), nullable=True),
        pa.field("sponsor_name", pa.string(), nullable=True),
        pa.field("sponsor_class", pa.string(), nullable=True),
        pa.field("organization_name", pa.string(), nullable=True),
        pa.field("organization_class", pa.string(), nullable=True),
        pa.field("overall_status", pa.string(), nullable=True),
        pa.field("completion_date", pa.string(), nullable=True),
        pa.field("completion_date_type", pa.string(), nullable=True),
        pa.field("primary_completion_date", pa.string(), nullable=True),
        pa.field("study_type", pa.string(), nullable=True),
        pa.field("phases", pa.string(), nullable=True),
        pa.field("enrollment_count", pa.int32(), nullable=True),
        pa.field("enrollment_type", pa.string(), nullable=True),
        pa.field("has_results", pa.bool_(), nullable=True),
        pa.field("conditions", pa.string(), nullable=True),
        pa.field("interventions", pa.string(), nullable=True),
        pa.field("last_update_date", pa.string(), nullable=True),
        pa.field("study_first_submit_date", pa.string(), nullable=True),
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


def check_schema_compatible(table, expected_schema: Schema) -> None:
    """
    Check if table schema matches expected schema.

    PyIceberg's upsert has a bug (#2467) where it fails when data files
    have a different schema than the incoming data. Schema evolution
    updates metadata but doesn't rewrite data files, so upserts fail.

    If schemas don't match, raise an error instructing to use force_full.
    """
    current_field_names = {f.name for f in table.schema().fields}
    expected_field_names = {f.name for f in expected_schema.fields}

    missing = expected_field_names - current_field_names
    extra = current_field_names - expected_field_names

    if missing or extra:
        msg = "[schema] Schema mismatch detected!\n"
        if missing:
            msg += f"  Missing columns in table: {missing}\n"
        if extra:
            msg += f"  Extra columns in table: {extra}\n"
        msg += "  PyIceberg upsert cannot handle schema changes (issue #2467).\n"
        msg += "  Re-run with force_full=true to rebuild the table."
        log(msg)
        raise ValueError(msg)


def studies_to_arrow(studies: list[dict]) -> pa.Table:
    """
    Convert study dicts to PyArrow table.

    Deduplicates by nct_id (last occurrence wins).
    """
    seen = {}
    for s in studies:
        nct_id = s.get("nct_id")
        if nct_id:
            seen[nct_id] = s

    return pa.Table.from_pylist(list(seen.values()), schema=STUDY_ARROW_SCHEMA)


def table_exists(
    table_bucket_arn: str,
    namespace: str = "clinical",
    table_name: str = "trials",
    region: str = "us-east-1",
) -> bool:
    """Check if the trials table exists."""
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


def load_studies(
    studies: list[dict],
    table_bucket_arn: str,
    namespace: str = "clinical",
    table_name: str = "trials",
    region: str = "us-east-1",
    batch_size: int = 5000,
    force_full: bool = False,
) -> dict:
    """
    Load studies to S3 Tables Iceberg table using upsert.

    Creates namespace and table if they don't exist.
    Uses PyIceberg's upsert which automatically detects unchanged rows.

    Args:
        studies: List of study dicts from extract
        table_bucket_arn: ARN of the S3 Table Bucket
        namespace: Iceberg namespace (default: clinical)
        table_name: Table name (default: trials)
        region: AWS region
        batch_size: Number of rows per upsert batch

    Returns:
        Dict with rows_inserted and rows_updated counts
    """
    import os
    import traceback

    log(f"[load] Starting load_studies with {len(studies)} studies")

    batch_size = int(os.environ.get("UPSERT_BATCH_SIZE", batch_size))
    log(f"[load] batch_size={batch_size}")

    log("[load] Getting catalog...")
    catalog = get_catalog(table_bucket_arn, region)
    log("[load] Catalog obtained")

    ensure_namespace(catalog, namespace)

    table_id = f"{namespace}.{table_name}"

    log("[load] Converting to Arrow table...")
    arrow_table = studies_to_arrow(studies)
    deduped_count = len(arrow_table)

    if deduped_count != len(studies):
        dupes = len(studies) - deduped_count
        log(f"[load] Incoming: {len(studies)} studies ({dupes} duplicates removed)")
    else:
        log(f"[load] Incoming: {deduped_count} studies")

    log(f"[load] Arrow table memory: {arrow_table.nbytes / 1024 / 1024:.2f} MB")
    log_memory("post-arrow-convert")

    total_inserted = 0
    total_updated = 0

    try:
        table = catalog.load_table(table_id)
        log(f"[load] Table exists: {table_id}")

        # For force_full, drop and recreate the table to ensure clean schema
        if force_full:
            log("[load] force_full=True, dropping table for fresh load...")
            catalog.drop_table(table_id, purge_requested=True)
            raise NoSuchTableError(table_id)  # Fall through to create branch

        # Check schema compatibility (pyiceberg upsert can't handle mismatches)
        check_schema_compatible(table, STUDY_SCHEMA)

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
                result = table.upsert(batch, join_cols=["nct_id"])
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
        table = catalog.create_table(table_id, schema=STUDY_SCHEMA)
        log("[load] Table created, appending data...")
        table.append(arrow_table)
        log(f"[load] Initial load complete: {deduped_count} studies")
        log_memory("complete")
        return {"rows_inserted": deduped_count, "rows_updated": 0}

    except Exception as e:
        log(f"[load] FATAL ERROR: {type(e).__name__}: {e}")
        log(f"[load] Traceback:\n{traceback.format_exc()}")
        raise
