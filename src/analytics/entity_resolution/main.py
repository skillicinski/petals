"""Entity resolution analysis - match clinical trial sponsors to public companies.

An analytics workload that uses machine learning to resolve entity names across
datasets. Unlike pipelines (which move data on schedules), this is an analytical
process for building matching datasets and models.

Approach:
---------
1. Load entities from data warehouse (S3 Tables via Athena)
2. Token-based blocking to reduce candidate space
3. Semantic embeddings (sentence-transformers)
4. Similarity scoring (cosine distance)
5. Optimal bipartite matching (Hungarian algorithm)
6. Results saved locally for evaluation and iteration

This is model development code, not production ETL. Results can be materialized
to warehouse tables after validation, but the primary workflow is:
  warehouse → local analysis → evaluation → iterate

Environment variables:
    TABLE_BUCKET_ARN: S3 Tables warehouse ARN
    OUTPUT_PATH: Local output path (default: data/entity_matches.parquet)
    LIMIT_SPONSORS: Max sponsors for development (default: all)
    LIMIT_TICKERS: Max tickers for development (default: all)
    EMBEDDING_MODEL: sentence-transformers model (default: all-MiniLM-L6-v2)
    SKIP_BLOCKING: Skip token pre-filter (default: false)
    MATCHING_ALGORITHM: 'greedy' or 'hungarian' (default: hungarian)

Usage:
    # Development mode (limited data)
    AWS_PROFILE=personal LIMIT_SPONSORS=100 LIMIT_TICKERS=200 \
        python -m src.analytics.entity_resolution.main

    # Full analysis
    AWS_PROFILE=personal python -m src.analytics.entity_resolution.main
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
from .load import load_sponsors, load_tickers
from .matching import greedy_matching, hungarian_matching

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

        print(f"Loading embedding model: {model_name}")
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

    print(f"Sponsors with candidates: {len(pair_candidates)}/{len(left_df)}")
    print(
        f"Candidate pairs: {total_pairs:,} (reduced from {naive_pairs:,}, "
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
    print(f"No blocking: {total_pairs:,} pairs to score (full cartesian product)")

    return pair_candidates


def select_best_matches(
    all_pairs_df: pl.DataFrame,
    left_key: str = "sponsor_name",
    right_key: str = "ticker",
    min_confidence: float = 0.0,
    algorithm: str = "hungarian",
) -> pl.DataFrame:
    """Select optimal 1:1 matches using specified algorithm.

    Args:
        all_pairs_df: All candidate pairs with scores
        left_key: Column name for left entities (sponsors)
        right_key: Column name for right entities (tickers)
        min_confidence: Minimum confidence threshold
        algorithm: 'greedy' or 'hungarian' (default: hungarian)

    Returns:
        DataFrame with selected 1:1 matches

    Matching Algorithms:
    -------------------
    - greedy: Fast (O(n²)), locally optimal, baseline
    - hungarian: Slower (O(n³)), globally optimal, recommended
    """
    if algorithm == "hungarian":
        result = hungarian_matching(
            all_pairs_df,
            left_key=left_key,
            right_key=right_key,
            score_key="confidence",
            min_score=min_confidence,
        )
    elif algorithm == "greedy":
        result = greedy_matching(
            all_pairs_df,
            left_key=left_key,
            right_key=right_key,
            score_key="confidence",
            min_score=min_confidence,
        )
    else:
        raise ValueError(f"Unknown matching algorithm: {algorithm}. Use 'greedy' or 'hungarian'")

    print(f"1:1 matching ({algorithm}): {len(result)} unique pairs")

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
        print("No candidates to score")
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

    print(f"Computing embeddings:")
    print(f"  Sponsors: {len(left_texts)}")
    print(f"  Tickers: {len(right_texts)}")

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

    print(f"Scored {len(df)} candidate pairs")
    print(
        f"  High confidence (≥{CONFIDENCE_AUTO_APPROVE:.0%}): {(df['confidence'] >= CONFIDENCE_AUTO_APPROVE).sum()}"
    )
    print(
        f"  Medium confidence: {((df['confidence'] >= CONFIDENCE_AUTO_REJECT) & (df['confidence'] < CONFIDENCE_AUTO_APPROVE)).sum()}"
    )
    print(
        f"  Low confidence (<{CONFIDENCE_AUTO_REJECT:.0%}): {(df['confidence'] < CONFIDENCE_AUTO_REJECT).sum()}"
    )

    return df


def generate_analysis_id() -> str:
    """Generate unique identifier for this analysis run."""
    return str(uuid.uuid4())


def analyze_entity_matches(
    limit_sponsors: int | None = None,
    limit_tickers: int | None = None,
    model_name: str = "all-MiniLM-L6-v2",
    output_path: str = "data/entity_matches.parquet",
    skip_blocking: bool = False,
    matching_algorithm: str = "hungarian",
) -> pl.DataFrame:
    """Analyze and generate entity match candidates between sponsors and tickers.

    This is an analytics workload, not a pipeline. It loads data from the warehouse,
    applies ML matching, and saves results locally for evaluation/iteration.

    Args:
        limit_sponsors: Limit sponsors (for development/testing)
        limit_tickers: Limit tickers (for development/testing)
        model_name: Sentence-transformer embedding model
        output_path: Local output path for results
        skip_blocking: Skip token pre-filter (slower, more complete)
        matching_algorithm: 'greedy' or 'hungarian' (default: hungarian)

    Returns:
        DataFrame with 1:1 matched sponsor-ticker pairs
    """
    start_time = time.time()
    analysis_id = generate_analysis_id()

    print("=" * 70)
    print("ENTITY RESOLUTION ANALYSIS")
    print("=" * 70)
    print(f"Analysis ID: {analysis_id}")
    print(f"Model: {model_name}")
    print(f"Confidence thresholds:")
    print(f"  Auto-approve: ≥{CONFIDENCE_AUTO_APPROVE:.0%}")
    print(f"  Auto-reject:  <{CONFIDENCE_AUTO_REJECT:.0%}")
    print(f"Blocking: {'disabled (full cartesian)' if skip_blocking else 'enabled (token-based)'}")
    print(f"Matching algorithm: {matching_algorithm}")

    # === Load from Warehouse ===
    print("\n" + "=" * 70)
    print("LOAD DATA FROM WAREHOUSE")
    print("=" * 70)
    sponsors_df = load_sponsors(limit=limit_sponsors)
    tickers_df = load_tickers(limit=limit_tickers)

    # === Token-based Blocking ===
    print("\n" + "=" * 70)
    print("CANDIDATE GENERATION (BLOCKING)")
    print("=" * 70)
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
        print("No candidate pairs generated. Exiting.")
        return pl.DataFrame()

    # === Embedding & Scoring ===
    print("\n" + "=" * 70)
    print("SIMILARITY SCORING")
    print("=" * 70)
    all_pairs_df = score_pairs(
        sponsors_df,
        tickers_df,
        pair_candidates,
        left_key="sponsor_name",
        right_key="ticker",
        right_text="name",
        model_name=model_name,
    )

    # === Optimal Matching ===
    print("\n" + "=" * 70)
    print("1:1 MATCHING")
    print("=" * 70)
    matches_df = select_best_matches(
        all_pairs_df,
        left_key="sponsor_name",
        right_key="ticker",
        min_confidence=CONFIDENCE_AUTO_REJECT,
        algorithm=matching_algorithm,
    )

    # Add metadata
    from datetime import datetime, timezone

    matches_df = matches_df.with_columns(
        [
            pl.lit(analysis_id).alias("analysis_id"),
            pl.lit(datetime.now(timezone.utc)).alias("created_at"),
        ]
    )

    # Rename columns for clarity
    if "name" in matches_df.columns:
        matches_df = matches_df.rename({"name": "ticker_name"})

    # === Save Results Locally ===
    print("\n" + "=" * 70)
    print("SAVE RESULTS")
    print("=" * 70)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    matches_df.write_parquet(output_path)
    print(f"Saved to: {output_path}")
    print(f"Total matches: {len(matches_df)}")

    # Summary statistics
    elapsed = time.time() - start_time
    approved = (matches_df["status"] == STATUS_APPROVED).sum()
    pending = (matches_df["status"] == STATUS_PENDING).sum()
    avg_confidence = matches_df["confidence"].mean()

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)
    print(f"Time: {elapsed:.1f}s")
    print(f"Analysis ID: {analysis_id}")
    print(f"Total matches: {len(matches_df)}")
    print(f"  Auto-approved (≥{CONFIDENCE_AUTO_APPROVE:.0%}): {approved}")
    print(f"  Pending review: {pending}")
    print(f"  Average confidence: {avg_confidence:.3f}")

    # Show top matches
    if len(matches_df) > 0:
        print(f"\nTop 15 highest-confidence matches:")
        for i, row in enumerate(matches_df.head(15).iter_rows(named=True), 1):
            icon = "✓" if row["status"] == STATUS_APPROVED else "?"
            ticker_name = row.get("ticker_name", "")[:30]
            print(
                f"  {i:2}. {icon} {row['confidence']:.3f} | "
                f"{row['sponsor_name'][:40]:<40} → {row['ticker']:6} ({ticker_name})"
            )

    print(f"\n💡 Next steps:")
    print(f"  1. Evaluate: python scripts/evaluate_predictions.py --predictions {output_path}")
    print(f"  2. Label more: python scripts/label_helper.py")
    print(f"  3. Iterate: Adjust blocking, embeddings, or thresholds")

    return matches_df


if __name__ == "__main__":
    # Parse environment configuration
    limit_sponsors = int(os.environ.get("LIMIT_SPONSORS", 0)) or None
    limit_tickers = int(os.environ.get("LIMIT_TICKERS", 0)) or None
    model_name = os.environ.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    output_path = os.environ.get("OUTPUT_PATH", "data/entity_matches.parquet")
    skip_blocking = os.environ.get("SKIP_BLOCKING", "0") == "1"
    matching_algorithm = os.environ.get("MATCHING_ALGORITHM", "hungarian")

    # Run analysis
    analyze_entity_matches(
        limit_sponsors=limit_sponsors,
        limit_tickers=limit_tickers,
        model_name=model_name,
        output_path=output_path,
        skip_blocking=skip_blocking,
        matching_algorithm=matching_algorithm,
    )
