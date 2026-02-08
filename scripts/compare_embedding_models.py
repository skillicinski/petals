"""Compare different embedding models for entity resolution.

This script:
1. Loads sponsors and tickers from warehouse
2. Runs entity resolution with multiple embedding models
3. Evaluates each model against ground truth
4. Compares accuracy, speed, and resource usage
5. Recommends the best model

Usage:
    # Test default models (MiniLM, MPNet, BioBERT)
    AWS_PROFILE=personal uv run python scripts/compare_embedding_models.py

    # Test specific models
    AWS_PROFILE=personal uv run python scripts/compare_embedding_models.py --models minilm mpnet

    # Use limited data for quick testing
    AWS_PROFILE=personal LIMIT_SPONSORS=100 LIMIT_TICKERS=200 \
        uv run python scripts/compare_embedding_models.py
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import polars as pl

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.entity_resolution.evaluation import (
    compute_metrics,
    load_ground_truth,
)
from src.analytics.entity_resolution.load import load_sponsors, load_tickers
from src.analytics.entity_resolution.main import (
    generate_all_pairs,
    pre_filter_by_tokens,
    score_pairs,
    select_best_matches,
)
from src.analytics.entity_resolution.models import (
    AVAILABLE_MODELS,
    get_model_config,
)
from src.analytics.entity_resolution.config import CONFIDENCE_AUTO_REJECT


def run_with_model(
    model_name: str,
    sponsors_df: pl.DataFrame,
    tickers_df: pl.DataFrame,
    skip_blocking: bool = False,
) -> tuple[pl.DataFrame, float]:
    """Run entity resolution with a specific model.

    Args:
        model_name: Name of sentence-transformer model
        sponsors_df: Sponsor entities
        tickers_df: Ticker entities
        skip_blocking: Skip token pre-filter

    Returns:
        Tuple of (matches DataFrame, elapsed_time_seconds)
    """
    start_time = time.time()

    # Blocking
    if skip_blocking:
        pair_candidates = generate_all_pairs(sponsors_df, tickers_df)
    else:
        pair_candidates = pre_filter_by_tokens(
            sponsors_df, tickers_df, left_key="sponsor_name", right_key="ticker", right_text="name"
        )

    if not pair_candidates:
        return pl.DataFrame(), time.time() - start_time

    # Score with embeddings
    all_pairs_df = score_pairs(
        sponsors_df,
        tickers_df,
        pair_candidates,
        model_name=model_name,
    )

    # Optimal matching
    matches_df = select_best_matches(
        all_pairs_df,
        min_confidence=CONFIDENCE_AUTO_REJECT,
        algorithm="hungarian",
    )

    elapsed = time.time() - start_time

    return matches_df, elapsed


def main():
    parser = argparse.ArgumentParser(description="Compare embedding models for entity resolution")
    parser.add_argument(
        "--models",
        nargs="+",
        choices=list(AVAILABLE_MODELS.keys()),
        default=["minilm", "mpnet", "biobert"],
        help="Models to compare (default: minilm, mpnet, biobert)",
    )
    parser.add_argument(
        "--skip-blocking",
        action="store_true",
        help="Skip token pre-filter (slower, more complete)",
    )
    parser.add_argument(
        "--output",
        default="data/model_comparison.json",
        help="Output path for comparison results",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("EMBEDDING MODEL COMPARISON")
    print("=" * 80)

    # Get limits from environment
    limit_sponsors = int(os.environ.get("LIMIT_SPONSORS", 0)) or None
    limit_tickers = int(os.environ.get("LIMIT_TICKERS", 0)) or None

    # Load data
    print("\n📊 Loading data from warehouse...")
    sponsors_df = load_sponsors(limit=limit_sponsors)
    tickers_df = load_tickers(limit=limit_tickers)
    ground_truth = load_ground_truth()

    print(f"Sponsors: {len(sponsors_df)}")
    print(f"Tickers: {len(tickers_df)}")
    print(f"Ground truth labels: {len(ground_truth)}")

    # Run comparison
    results = []
    print(f"\n🔬 Testing {len(args.models)} models...")
    print("=" * 80)

    for i, model_key in enumerate(args.models, 1):
        config = get_model_config(model_key)

        print(f"\n[{i}/{len(args.models)}] {config.display_name}")
        print("-" * 80)
        print(f"Model: {config.name}")
        print(f"Domain: {config.domain}")
        print(f"Size: {config.size_mb}MB, Embedding dim: {config.embedding_dim}")
        print(f"Description: {config.description}")
        print()

        # Run entity resolution
        try:
            matches_df, elapsed = run_with_model(
                config.name,
                sponsors_df,
                tickers_df,
                skip_blocking=args.skip_blocking,
            )

            # Evaluate
            metrics = compute_metrics(matches_df, ground_truth, min_confidence=0.0)

            result = {
                "model_key": model_key,
                "model_name": config.name,
                "display_name": config.display_name,
                "domain": config.domain,
                "size_mb": config.size_mb,
                "embedding_dim": config.embedding_dim,
                "elapsed_seconds": round(elapsed, 2),
                "total_matches": len(matches_df),
                "precision": round(metrics.precision, 4),
                "recall": round(metrics.recall, 4),
                "f1": round(metrics.f1, 4),
                "coverage": round(metrics.coverage, 4),
                "true_positives": metrics.true_positives,
                "false_positives": metrics.false_positives,
                "false_negatives": metrics.false_negatives,
            }

            results.append(result)

            # Print results
            print(f"⏱️  Time: {elapsed:.1f}s")
            print(f"📈 Results:")
            print(f"   Matches: {len(matches_df)}")
            print(f"   Precision: {metrics.precision:.4f} ({metrics.true_positives}/{metrics.true_positives + metrics.false_positives})")
            print(f"   Recall:    {metrics.recall:.4f} ({metrics.true_positives}/{metrics.true_positives + metrics.false_negatives})")
            print(f"   F1 Score:  {metrics.f1:.4f}")
            print(f"   Coverage:  {metrics.coverage:.1%}")

        except Exception as e:
            print(f"❌ Error: {e}")
            continue

    # Summary comparison
    print("\n" + "=" * 80)
    print("COMPARISON SUMMARY")
    print("=" * 80)

    if not results:
        print("No results to compare")
        return

    # Sort by F1 score
    results_sorted = sorted(results, key=lambda x: x["f1"], reverse=True)

    print("\n📊 Ranking by F1 Score:")
    print("-" * 80)
    for i, r in enumerate(results_sorted, 1):
        icon = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        print(f"{icon} {r['display_name']:20} | F1: {r['f1']:.4f} | P: {r['precision']:.4f} | R: {r['recall']:.4f} | {r['elapsed_seconds']}s")

    # Best model
    best = results_sorted[0]
    print(f"\n🏆 WINNER: {best['display_name']}")
    print(f"   F1 Score: {best['f1']:.4f}")
    print(f"   Precision: {best['precision']:.4f}")
    print(f"   Recall: {best['recall']:.4f}")
    print(f"   Time: {best['elapsed_seconds']}s")

    # Speed vs accuracy tradeoff
    baseline = next((r for r in results if r["model_key"] == "minilm"), None)
    if baseline and best["model_key"] != "minilm":
        f1_gain = (best["f1"] - baseline["f1"]) / baseline["f1"] * 100
        time_cost = (best["elapsed_seconds"] - baseline["elapsed_seconds"]) / baseline["elapsed_seconds"] * 100
        print(f"\n📈 Improvement vs Baseline (MiniLM):")
        print(f"   F1 Score: {f1_gain:+.1f}%")
        print(f"   Time cost: {time_cost:+.1f}%")

    # Precision vs Recall analysis
    print("\n⚖️  Precision vs Recall Tradeoffs:")
    print("-" * 80)
    for r in results_sorted:
        if r["precision"] > r["recall"]:
            bias = "High precision (fewer FP, more FN)"
        elif r["recall"] > r["precision"]:
            bias = "High recall (fewer FN, more FP)"
        else:
            bias = "Balanced"
        print(f"{r['display_name']:20} | {bias}")

    # Save results
    output_path = Path(args.output)
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(
            {
                "comparison_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "data_size": {
                    "sponsors": len(sponsors_df),
                    "tickers": len(tickers_df),
                    "ground_truth": len(ground_truth),
                },
                "results": results,
                "winner": best,
            },
            f,
            indent=2,
        )

    print(f"\n💾 Results saved to: {output_path}")
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
