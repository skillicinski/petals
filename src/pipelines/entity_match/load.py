"""Load match candidates to storage."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow as pa
from pyiceberg.catalog import load_catalog

from .config import OUTPUT_TABLE


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


def ensure_namespace(catalog, namespace: str) -> None:
    """Create namespace if it doesn't exist."""
    try:
        catalog.create_namespace(namespace)
        print(f"[load] Created namespace: {namespace}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"[load] Namespace exists: {namespace}")
        else:
            raise


def add_metadata_columns(df: pl.DataFrame, run_id: str) -> pl.DataFrame:
    """Add metadata columns for production tracking.

    Adds:
        - run_id: pipeline execution identifier (for CloudWatch correlation)
        - created_at: timestamp when this row was generated
        - reviewed_at: null (will be set when status changes)
    """
    now = datetime.now(timezone.utc)

    return df.with_columns(
        [
            pl.lit(run_id).alias("run_id"),
            pl.lit(now).alias("created_at"),
            pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("reviewed_at"),
        ]
    )


def rename_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Rename columns for clarity in output table."""
    if "name" in df.columns:
        df = df.rename({"name": "ticker_name"})
    return df


def select_output_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Select and order columns for output."""
    output_cols = [
        # Match identification
        "sponsor_name",
        "ticker",
        "market",
        "ticker_name",
        # Match context
        "sponsor_aliases",
        "ticker_aliases",
        "match_reason",
        # Scoring
        "confidence",
        # Review status
        "status",
        "approved_by",
        "rejected_by",
        # Timestamps
        "created_at",
        "reviewed_at",
        # Pipeline metadata
        "run_id",
    ]
    # Only select columns that exist
    available = [c for c in output_cols if c in df.columns]
    return df.select(available)


def save_candidates_json(df: pl.DataFrame, output_path: str | Path) -> None:
    """Save candidates to local JSON file for inspection."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert to list of dicts
    records = df.to_dicts()

    with open(output_path, "w") as f:
        json.dump(records, f, indent=2, default=str)

    print(f"[load] Saved {len(records)} candidates to {output_path}")


def save_candidates_parquet(df: pl.DataFrame, output_path: str | Path) -> None:
    """Save candidates to local Parquet file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.write_parquet(output_path)
    print(f"[load] Saved {len(df)} candidates to {output_path}")


def save_candidates_s3(df: pl.DataFrame, run_id: str) -> str | None:
    """Save candidates to S3 artifacts bucket for recovery.

    Writes to s3://{ARTIFACTS_BUCKET}/recovery/entity_match/{run_id}/candidates.parquet

    Returns the S3 URI if successful, None if ARTIFACTS_BUCKET not set.
    """
    bucket = os.environ.get("ARTIFACTS_BUCKET")
    if not bucket:
        print("[load] ARTIFACTS_BUCKET not set, skipping S3 staging")
        return None

    s3_uri = f"s3://{bucket}/recovery/entity_match/{run_id}/candidates.parquet"
    df.write_parquet(s3_uri)
    print(f"[load] Staged {len(df)} candidates to {s3_uri}")
    return s3_uri


def save_candidates_iceberg(df: pl.DataFrame) -> None:
    """Save candidates to Iceberg table (append mode).

    Creates table if it doesn't exist.
    Appends new rows (preserves history for audit trail).
    """
    catalog = get_catalog()

    namespace = OUTPUT_TABLE.split(".")[0]
    ensure_namespace(catalog, namespace)

    # Check if table exists, create if not
    try:
        table = catalog.load_table(OUTPUT_TABLE)
    except Exception:
        print(f"[load] Creating {OUTPUT_TABLE} table...")
        schema = pa.schema(
            [
                # Match identification
                ("sponsor_name", pa.string()),
                ("ticker", pa.string()),
                ("market", pa.string()),
                ("ticker_name", pa.string()),
                # Match context
                ("sponsor_aliases", pa.list_(pa.string())),
                ("ticker_aliases", pa.list_(pa.string())),
                ("match_reason", pa.string()),
                # Scoring
                ("confidence", pa.float64()),
                # Review status
                ("status", pa.string()),
                ("approved_by", pa.string()),
                ("rejected_by", pa.string()),
                # Timestamps
                ("created_at", pa.timestamp("us", tz="UTC")),
                ("reviewed_at", pa.timestamp("us", tz="UTC")),
                # Pipeline metadata
                ("run_id", pa.string()),
            ]
        )
        table = catalog.create_table(
            OUTPUT_TABLE, schema=schema, properties={"format-version": "2"}
        )

    # Convert to Arrow and append
    arrow_table = df.to_arrow()
    table.append(arrow_table)

    print(f"[load] Appended {len(df)} candidates to {OUTPUT_TABLE}")


def prepare_output(df: pl.DataFrame, run_id: str) -> pl.DataFrame:
    """Prepare DataFrame for output with all metadata."""
    df = rename_columns(df)
    df = add_metadata_columns(df, run_id)
    df = select_output_columns(df)
    return df
