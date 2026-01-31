"""Entity matching pipeline - main entry point.

Matches clinical trial sponsors to public company tickers using
LLM-generated aliases and fuzzy matching.

Environment variables:
    TABLE_BUCKET_ARN: S3 Tables bucket ARN
    LLM_BACKEND: 'ollama' (local) or 'bedrock' (cloud)
    OLLAMA_MODEL: Model name for Ollama (default: llama3.2:3b)
    BEDROCK_MODEL: Model ID for Bedrock (default: meta.llama3-8b-instruct-v1:0)
    OUTPUT_PATH: Path for JSON/Parquet output (default: data/candidates.parquet)
    LIMIT_SPONSORS: Max sponsors to process (default: all)
    LIMIT_TICKERS: Max tickers to process (default: all)
    SAVE_ICEBERG: Set to '1' to save to Iceberg table
    RUN_ID: Pipeline run identifier (default: auto-generated UUID)

Usage:
    # Local with Ollama (start `ollama serve` first)
    AWS_PROFILE=personal LLM_BACKEND=ollama LIMIT_SPONSORS=50 LIMIT_TICKERS=100 \\
        python -m src.pipelines.entity_match.main

    # Cloud with Bedrock
    LLM_BACKEND=bedrock python -m src.pipelines.entity_match.main
"""

import os
import time
import uuid

from .aliases import enrich_with_aliases
from .blocking import generate_candidates
from .config import (
    CONFIDENCE_AUTO_APPROVE,
    CONFIDENCE_REVIEW_HIGH,
    CONFIDENCE_REVIEW_LOW,
    STATUS_APPROVED,
    STATUS_PENDING,
    STATUS_REJECTED,
)
from .extract import fetch_sponsors, fetch_tickers
from .load import (
    ensure_namespace,
    get_catalog,
    prepare_output,
    save_candidates_iceberg,
    save_candidates_json,
    save_candidates_parquet,
    save_candidates_s3,
)
from .scoring import score_candidates


def generate_run_id() -> str:
    """Generate a unique run ID for this pipeline execution.

    Uses RUN_ID env var if set (e.g., from Step Functions execution ID),
    otherwise generates a UUID.

    The run_id can be used to correlate:
    - Output rows in the candidates table
    - CloudWatch logs for this execution
    - Step Functions execution history
    """
    return os.environ.get("RUN_ID") or str(uuid.uuid4())


def run_pipeline():
    """Run the entity matching pipeline."""
    start_time = time.time()
    run_id = generate_run_id()

    # Configuration
    limit_sponsors = int(os.environ.get("LIMIT_SPONSORS", 0)) or None
    limit_tickers = int(os.environ.get("LIMIT_TICKERS", 0)) or None
    output_path = os.environ.get("OUTPUT_PATH", "data/candidates.parquet")
    save_iceberg = os.environ.get("SAVE_ICEBERG", "0") == "1"
    backend = os.environ.get("LLM_BACKEND", "ollama")

    print(f"[main] Starting entity matching pipeline")
    print(f"[main] Run ID: {run_id}")
    print(f"[main] Backend: {backend}")
    print(f"[main] Limits: sponsors={limit_sponsors or 'all'}, tickers={limit_tickers or 'all'}")
    print(
        f"[main] Thresholds: auto-approve≥{CONFIDENCE_AUTO_APPROVE:.0%}, "
        f"review={CONFIDENCE_REVIEW_LOW:.0%}-{CONFIDENCE_REVIEW_HIGH:.0%}"
    )

    # === Pre-flight checks ===
    # Validate Iceberg namespace exists early to fail-fast before expensive LLM work
    if save_iceberg:
        print("\n[main] === PRE-FLIGHT CHECKS ===")
        from .config import OUTPUT_TABLE

        namespace = OUTPUT_TABLE.split(".")[0]
        catalog = get_catalog()
        ensure_namespace(catalog, namespace)
        print(f"[main] Iceberg namespace ready: {namespace}")

    # === Extract ===
    print("\n[main] === EXTRACT ===")
    sponsors_df = fetch_sponsors(limit=limit_sponsors)
    tickers_df = fetch_tickers(limit=limit_tickers)

    # === Generate Aliases ===
    print("\n[main] === ALIAS GENERATION ===")

    print(f"[main] Generating aliases for {len(sponsors_df)} sponsors...")
    sponsors_df = enrich_with_aliases(
        sponsors_df,
        name_col="sponsor_name",
        backend=backend,
    )

    print(f"[main] Generating aliases for {len(tickers_df)} tickers...")
    tickers_df = enrich_with_aliases(
        tickers_df,
        name_col="name",
        description_col="description",
        backend=backend,
    )

    # === Blocking ===
    print("\n[main] === BLOCKING ===")
    candidates_df = generate_candidates(sponsors_df, tickers_df)

    if len(candidates_df) == 0:
        print("[main] No candidates generated. Exiting.")
        return None

    # === Scoring ===
    print("\n[main] === SCORING ===")
    candidates_df = score_candidates(candidates_df)

    # === Prepare Output ===
    print("\n[main] === LOAD ===")
    candidates_df = prepare_output(candidates_df, run_id)

    # Stage to S3 first (for recovery if Iceberg fails)
    s3_uri = save_candidates_s3(candidates_df, run_id)

    # Save locally only if not running in cloud (no S3 staging)
    if not s3_uri:
        if output_path.endswith(".json"):
            save_candidates_json(candidates_df, output_path)
        else:
            save_candidates_parquet(candidates_df, output_path)

    # Save to Iceberg
    if save_iceberg:
        save_candidates_iceberg(candidates_df)

    # Summary
    elapsed = time.time() - start_time
    auto_approved = (candidates_df["status"] == STATUS_APPROVED).sum()
    pending = (candidates_df["status"] == STATUS_PENDING).sum()
    auto_rejected = (candidates_df["status"] == STATUS_REJECTED).sum()

    print(f"\n[main] Pipeline complete in {elapsed:.1f}s")
    print(f"[main] Run ID: {run_id}")
    print(f"[main] Results: {len(candidates_df)} candidates")
    print(f"[main]   - Auto-approved: {auto_approved}")
    print(f"[main]   - Pending review: {pending}")
    print(f"[main]   - Auto-rejected: {auto_rejected}")
    print(f"[main] Output: {s3_uri or output_path}")

    return candidates_df


if __name__ == "__main__":
    run_pipeline()
