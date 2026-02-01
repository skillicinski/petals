"""Entity matching pipeline - embedding-based approach.

Matches clinical trial sponsors to public company tickers using
sentence-transformer embeddings and cosine similarity.

Flow:
1. Extract sponsors and tickers from Iceberg tables
2. Token-based pre-filter (blocking) to reduce candidate pairs
3. Compute embeddings for filtered entities
4. Cosine similarity scoring
5. Optimal 1:1 bipartite matching (greedy)
6. Threshold-based classification

Environment variables:
    TABLE_BUCKET_ARN: S3 Tables bucket ARN
    OUTPUT_PATH: Path for Parquet output (default: data/candidates.parquet)
    LIMIT_SPONSORS: Max sponsors to process (default: all)
    LIMIT_TICKERS: Max tickers to process (default: all)
    SAVE_ICEBERG: Set to '1' to save to Iceberg table
    RUN_ID: Pipeline run identifier (default: auto-generated UUID)
    EMBEDDING_MODEL: Sentence-transformer model (default: all-MiniLM-L6-v2)
    SKIP_BLOCKING: Set to '1' to skip token pre-filter (slower but complete)

Usage:
    # Local development
    AWS_PROFILE=personal LIMIT_SPONSORS=100 LIMIT_TICKERS=200 \
        python -m src.pipelines.entity_match.main

    # Full run
    AWS_PROFILE=personal python -m src.pipelines.entity_match.main
"""

import os
import time
import uuid

import numpy as np
import polars as pl

from .blocking import build_token_index, tokenize
from .config import (
    CONFIDENCE_AUTO_APPROVE,
    CONFIDENCE_AUTO_REJECT,
    STATUS_APPROVED,
    STATUS_PENDING,
    STATUS_REJECTED,
)
from .extract import fetch_sponsors, fetch_tickers

# Lazy import to avoid loading torch until needed
_model = None


def get_embedding_model(model_name: str = "all-MiniLM-L6-v2"):
    """Load sentence-transformer model (cached after first call).

    Default model: all-MiniLM-L6-v2
    - Size: ~80MB
    - Embedding dim: 384
    - Speed: ~14k sentences/sec on CPU
    """
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer

        print(f"[main] Loading embedding model: {model_name}")
        _model = SentenceTransformer(model_name)
    return _model


def compute_embeddings(
    texts: list[str],
    model_name: str = "all-MiniLM-L6-v2",
    batch_size: int = 64,
    show_progress: bool = True,
) -> np.ndarray:
    """Compute embeddings for a list of texts.

    Returns numpy array of shape (len(texts), embedding_dim).
    Embeddings are L2-normalized for cosine similarity.
    """
    model = get_embedding_model(model_name)
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=show_progress,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )
    return embeddings


def pre_filter_by_tokens(
    left_df: pl.DataFrame,
    right_df: pl.DataFrame,
    left_key: str = "sponsor_name",
    right_key: str = "ticker",
    right_text: str = "name",
) -> dict[str, set[str]]:
    """Pre-filter entities by token overlap (blocking).

    Returns mapping of left_key -> set of candidate right_keys.
    Only pairs with shared significant tokens are included.
    """
    right_index = build_token_index(right_df, right_key, right_text)

    pair_candidates: dict[str, set[str]] = {}

    for row in left_df.iter_rows(named=True):
        left_entity = row[left_key]
        tokens = tokenize(left_entity)

        candidate_keys: set[str] = set()
        for token in tokens:
            if token in right_index:
                candidate_keys.update(right_index[token])

        if candidate_keys:
            pair_candidates[left_entity] = candidate_keys

    total_pairs = sum(len(v) for v in pair_candidates.values())
    naive_pairs = len(left_df) * len(right_df)
    reduction = (1 - total_pairs / naive_pairs) * 100 if naive_pairs > 0 else 0

    print(f"[main] Pre-filter: {len(pair_candidates)}/{len(left_df)} sponsors have candidates")
    print(
        f"[main] Candidate pairs: {total_pairs:,} (reduced from {naive_pairs:,}, "
        f"{reduction:.1f}% reduction)"
    )

    return pair_candidates


def generate_all_pairs(
    left_df: pl.DataFrame,
    right_df: pl.DataFrame,
    left_key: str = "sponsor_name",
    right_key: str = "ticker",
) -> dict[str, set[str]]:
    """Generate all possible pairs (no blocking).

    Use when you want to compare every sponsor with every ticker.
    Much slower but guarantees no matches are missed.
    """
    left_entities = left_df[left_key].to_list()
    right_entities = set(right_df[right_key].to_list())

    pair_candidates = {left: right_entities for left in left_entities}

    total_pairs = len(left_entities) * len(right_entities)
    print(f"[main] No pre-filter: {total_pairs:,} pairs to score")

    return pair_candidates


def select_best_matches(
    all_pairs_df: pl.DataFrame,
    left_key: str = "sponsor_name",
    right_key: str = "ticker",
    min_confidence: float = 0.0,
) -> pl.DataFrame:
    """Select optimal 1:1 matches using greedy assignment.

    For each sponsor, selects highest-confidence ticker not already matched.
    """
    df = all_pairs_df.filter(pl.col("confidence") >= min_confidence)

    if len(df) == 0:
        return df

    df = df.sort("confidence", descending=True)

    matched_left: set[str] = set()
    matched_right: set[str] = set()
    selected_rows = []

    for row in df.iter_rows(named=True):
        left_val = row[left_key]
        right_val = row[right_key]

        if left_val in matched_left or right_val in matched_right:
            continue

        matched_left.add(left_val)
        matched_right.add(right_val)
        selected_rows.append(row)

    if not selected_rows:
        return df.head(0)

    result = pl.DataFrame(selected_rows)
    print(f"[main] 1:1 matching: {len(result)} unique pairs")

    return result


def score_pairs(
    left_df: pl.DataFrame,
    right_df: pl.DataFrame,
    pair_candidates: dict[str, set[str]],
    left_key: str = "sponsor_name",
    right_key: str = "ticker",
    right_text: str = "name",
    model_name: str = "all-MiniLM-L6-v2",
) -> pl.DataFrame:
    """Score candidate pairs using embedding similarity."""
    left_entities = list(pair_candidates.keys())
    right_entities = list(set(k for keys in pair_candidates.values() for k in keys))

    if not left_entities or not right_entities:
        print("[main] No candidates to score")
        return pl.DataFrame(
            schema={
                "sponsor_name": pl.Utf8,
                "ticker": pl.Utf8,
                "name": pl.Utf8,
                "confidence": pl.Float64,
                "status": pl.Utf8,
            }
        )

    # Build lookup for right side data
    right_lookup = {
        row[right_key]: row
        for row in right_df.iter_rows(named=True)
        if row[right_key] in right_entities
    }

    # Extract texts for embedding
    left_texts = left_entities
    right_texts = [right_lookup[k][right_text] for k in right_entities]

    print(
        f"[main] Computing embeddings for {len(left_texts)} sponsors, {len(right_texts)} tickers..."
    )

    # Compute embeddings
    left_embeddings = compute_embeddings(left_texts, model_name)
    right_embeddings = compute_embeddings(right_texts, model_name)

    # Build index mappings
    left_idx = {name: i for i, name in enumerate(left_entities)}
    right_idx = {key: i for i, key in enumerate(right_entities)}

    # Score candidate pairs
    results = []
    for left_entity, candidate_rights in pair_candidates.items():
        left_i = left_idx[left_entity]
        left_emb = left_embeddings[left_i : left_i + 1]

        for right_ticker in candidate_rights:
            right_i = right_idx[right_ticker]
            right_emb = right_embeddings[right_i : right_i + 1]

            similarity = float(np.dot(left_emb, right_emb.T)[0, 0])

            if similarity >= CONFIDENCE_AUTO_APPROVE:
                status = STATUS_APPROVED
            elif similarity < CONFIDENCE_AUTO_REJECT:
                status = STATUS_REJECTED
            else:
                status = STATUS_PENDING

            right_data = right_lookup[right_ticker]
            results.append(
                {
                    "sponsor_name": left_entity,
                    "ticker": right_ticker,
                    "name": right_data[right_text],
                    "market": right_data.get("market"),
                    "confidence": round(similarity, 4),
                    "status": status,
                    "match_reason": f"embedding_similarity={similarity:.3f}",
                }
            )

    df = pl.DataFrame(results)
    df = df.sort("confidence", descending=True)

    print(f"[main] Scored {len(df)} candidate pairs")

    return df


def generate_run_id() -> str:
    """Generate unique run ID (from env or UUID)."""
    return os.environ.get("RUN_ID") or str(uuid.uuid4())


def run_pipeline(
    limit_sponsors: int | None = None,
    limit_tickers: int | None = None,
    model_name: str = "all-MiniLM-L6-v2",
    output_path: str = "data/candidates.parquet",
    skip_blocking: bool = False,
    save_iceberg: bool = False,
) -> pl.DataFrame:
    """Run the entity matching pipeline.

    Args:
        limit_sponsors: Max sponsors to process (for testing)
        limit_tickers: Max tickers to process (for testing)
        model_name: Sentence-transformer model
        output_path: Path for output parquet file
        skip_blocking: Skip token pre-filter (compare all pairs)
        save_iceberg: Save results to Iceberg table

    Returns:
        DataFrame with 1:1 matched candidates
    """
    start_time = time.time()
    run_id = generate_run_id()

    print("[main] === ENTITY MATCHING PIPELINE ===")
    print(f"[main] Run ID: {run_id}")
    print(f"[main] Model: {model_name}")
    print(
        f"[main] Thresholds: approve≥{CONFIDENCE_AUTO_APPROVE:.0%}, "
        f"reject<{CONFIDENCE_AUTO_REJECT:.0%}"
    )
    print(f"[main] Blocking: {'disabled' if skip_blocking else 'enabled'}")

    # === Extract ===
    print("\n[main] === EXTRACT ===")
    sponsors_df = fetch_sponsors(limit=limit_sponsors)
    tickers_df = fetch_tickers(limit=limit_tickers)

    # === Pre-filter (Blocking) ===
    print("\n[main] === BLOCKING ===")
    if skip_blocking:
        pair_candidates = generate_all_pairs(
            sponsors_df,
            tickers_df,
            left_key="sponsor_name",
            right_key="ticker",
        )
    else:
        pair_candidates = pre_filter_by_tokens(
            sponsors_df,
            tickers_df,
            left_key="sponsor_name",
            right_key="ticker",
            right_text="name",
        )

    if not pair_candidates:
        print("[main] No candidate pairs. Exiting.")
        return pl.DataFrame()

    # === Score with Embeddings ===
    print("\n[main] === SCORING ===")
    all_pairs_df = score_pairs(
        sponsors_df,
        tickers_df,
        pair_candidates,
        left_key="sponsor_name",
        right_key="ticker",
        right_text="name",
        model_name=model_name,
    )

    # === 1:1 Matching ===
    print("\n[main] === 1:1 MATCHING ===")
    candidates_df = select_best_matches(
        all_pairs_df,
        left_key="sponsor_name",
        right_key="ticker",
        min_confidence=CONFIDENCE_AUTO_REJECT,
    )

    # Add metadata and rename columns for output
    from datetime import datetime, timezone

    candidates_df = candidates_df.with_columns(
        [
            pl.lit(run_id).alias("run_id"),
            pl.lit(datetime.now(timezone.utc)).alias("created_at"),
        ]
    )

    # Rename 'name' to 'ticker_name' for clarity in output
    if "name" in candidates_df.columns:
        candidates_df = candidates_df.rename({"name": "ticker_name"})

    # === Save Output ===
    print("\n[main] === OUTPUT ===")
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    candidates_df.write_parquet(output_path)
    print(f"[main] Saved to {output_path}")

    # Save to Iceberg if requested
    if save_iceberg:
        from .load import save_candidates_iceberg

        save_candidates_iceberg(candidates_df)

    # Summary
    elapsed = time.time() - start_time
    approved = (candidates_df["status"] == STATUS_APPROVED).sum()
    pending = (candidates_df["status"] == STATUS_PENDING).sum()

    print("\n[main] === COMPLETE ===")
    print(f"[main] Time: {elapsed:.1f}s")
    print(f"[main] Run ID: {run_id}")
    print(f"[main] Results: {len(candidates_df)} unique 1:1 matches")
    print(f"[main]   - Auto-approved: {approved}")
    print(f"[main]   - Pending review: {pending}")

    # Top matches
    if len(candidates_df) > 0:
        print("\n[main] Top 15 matches:")
        for row in candidates_df.head(15).iter_rows(named=True):
            icon = "✓" if row["status"] == STATUS_APPROVED else "?"
            print(
                f"  {icon} {row['confidence']:.3f} | "
                f"{row['sponsor_name'][:40]:<40} → {row['ticker']} ({row['ticker_name'][:30]})"
            )

    return candidates_df


if __name__ == "__main__":
    limit_sponsors = int(os.environ.get("LIMIT_SPONSORS", 0)) or None
    limit_tickers = int(os.environ.get("LIMIT_TICKERS", 0)) or None
    model_name = os.environ.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    output_path = os.environ.get("OUTPUT_PATH", "data/candidates.parquet")
    skip_blocking = os.environ.get("SKIP_BLOCKING", "0") == "1"
    save_iceberg = os.environ.get("SAVE_ICEBERG", "0") == "1"

    run_pipeline(
        limit_sponsors=limit_sponsors,
        limit_tickers=limit_tickers,
        model_name=model_name,
        output_path=output_path,
        skip_blocking=skip_blocking,
        save_iceberg=save_iceberg,
    )
