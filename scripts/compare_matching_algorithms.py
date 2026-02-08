"""Compare greedy vs Hungarian matching algorithms on ground truth data.

This script:
1. Loads the latest predictions (from data/candidates.parquet)
2. Loads ground truth labels
3. Generates matches using both greedy and Hungarian algorithms
4. Evaluates precision, recall, F1 for each
5. Shows which algorithm performs better and why

Usage:
    uv run python scripts/compare_matching_algorithms.py

The script expects:
- data/candidates.parquet: Latest predictions with all sponsor-ticker pairs
- src/analytics/entity_resolution/data/ground_truth.csv: Ground truth labels
"""

import json
import polars as pl
from pathlib import Path
from datetime import datetime, timezone

from src.analytics.entity_resolution.evaluation import (
    load_ground_truth,
    compute_metrics,
)
from src.analytics.entity_resolution.matching import (
    greedy_matching,
    hungarian_matching,
    compare_matching_algorithms,
)
from src.analytics.entity_resolution.config import CONFIDENCE_AUTO_REJECT


def main():
    print("=" * 70)
    print("MATCHING ALGORITHM COMPARISON")
    print("=" * 70)

    # Load predictions (all pairs with scores)
    predictions_path = Path("data/candidates.parquet")
    if not predictions_path.exists():
        print(f"\nError: {predictions_path} not found")
        print("Run the entity resolution pipeline first:")
        print("  AWS_PROFILE=personal uv run python -m src.analytics.entity_resolution.main")
        return

    print(f"\nLoading predictions from: {predictions_path}")
    all_pairs = pl.read_parquet(predictions_path)
    print(f"Total pairs: {len(all_pairs)}")

    # Load ground truth
    print("\nLoading ground truth...")
    ground_truth = load_ground_truth()
    print(f"Ground truth labels: {len(ground_truth)}")

    # Filter to production threshold
    min_threshold = CONFIDENCE_AUTO_REJECT
    print(f"\nFiltering to production threshold: confidence >= {min_threshold:.2f}")
    filtered_pairs = all_pairs.filter(pl.col("confidence") >= min_threshold)
    print(f"Pairs above threshold: {len(filtered_pairs)} (from {len(all_pairs)})")

    # Compare matching algorithms (on scoring metrics)
    print("\n" + "=" * 70)
    print("ALGORITHM COMPARISON - Scoring Metrics")
    print("=" * 70)
    comparison = compare_matching_algorithms(filtered_pairs, min_score=0.0)

    print(f"\nGreedy Matching:")
    print(f"  Matches: {comparison['greedy_matches']}")
    print(f"  Total Score: {comparison['greedy_total_score']:.4f}")
    print(f"  Avg Score: {comparison['greedy_avg_score']:.4f}")

    print(f"\nHungarian Matching:")
    print(f"  Matches: {comparison['hungarian_matches']}")
    print(f"  Total Score: {comparison['hungarian_total_score']:.4f}")
    print(f"  Avg Score: {comparison['hungarian_avg_score']:.4f}")

    print(f"\nImprovement:")
    print(f"  Absolute: +{comparison['improvement']:.4f}")
    print(f"  Percentage: +{comparison['improvement_pct']:.2f}%")
    print(f"  Different Pairs: {comparison['different_pairs']}")

    # Evaluate on ground truth
    print("\n" + "=" * 70)
    print("ALGORITHM COMPARISON - Ground Truth Evaluation")
    print("=" * 70)

    # Greedy evaluation
    print("\n1. GREEDY MATCHING")
    print("-" * 70)
    greedy_matches = greedy_matching(filtered_pairs, min_score=0.0)
    greedy_metrics = compute_metrics(greedy_matches, ground_truth, min_confidence=0.0)

    print(f"Precision: {greedy_metrics.precision:.4f} ({greedy_metrics.true_positives}/{greedy_metrics.true_positives + greedy_metrics.false_positives})")
    print(f"Recall:    {greedy_metrics.recall:.4f} ({greedy_metrics.true_positives}/{greedy_metrics.true_positives + greedy_metrics.false_negatives})")
    print(f"F1 Score:  {greedy_metrics.f1:.4f}")
    print(f"Coverage:  {greedy_metrics.coverage:.1%}")

    # Hungarian evaluation
    print("\n2. HUNGARIAN MATCHING")
    print("-" * 70)
    hungarian_matches = hungarian_matching(filtered_pairs, min_score=0.0)
    hungarian_metrics = compute_metrics(hungarian_matches, ground_truth, min_confidence=0.0)

    print(f"Precision: {hungarian_metrics.precision:.4f} ({hungarian_metrics.true_positives}/{hungarian_metrics.true_positives + hungarian_metrics.false_positives})")
    print(f"Recall:    {hungarian_metrics.recall:.4f} ({hungarian_metrics.true_positives}/{hungarian_metrics.true_positives + hungarian_metrics.false_negatives})")
    print(f"F1 Score:  {hungarian_metrics.f1:.4f}")
    print(f"Coverage:  {hungarian_metrics.coverage:.1%}")

    # Comparison
    print("\n3. WINNER")
    print("-" * 70)

    f1_improvement = hungarian_metrics.f1 - greedy_metrics.f1
    precision_improvement = hungarian_metrics.precision - greedy_metrics.precision
    recall_improvement = hungarian_metrics.recall - greedy_metrics.recall

    if f1_improvement > 0:
        print("🏆 Hungarian Algorithm WINS!")
        print(f"\nImprovements:")
        print(f"  F1 Score:  +{f1_improvement:.4f} ({greedy_metrics.f1:.4f} → {hungarian_metrics.f1:.4f})")
        print(f"  Precision: {precision_improvement:+.4f} ({greedy_metrics.precision:.4f} → {hungarian_metrics.precision:.4f})")
        print(f"  Recall:    {recall_improvement:+.4f} ({greedy_metrics.recall:.4f} → {hungarian_metrics.recall:.4f})")
    elif f1_improvement < 0:
        print("🏆 Greedy Algorithm performs better (unexpected!)")
        print(f"\nF1 Score: {greedy_metrics.f1:.4f} (greedy) vs {hungarian_metrics.f1:.4f} (hungarian)")
    else:
        print("🤝 Tie - Both algorithms perform equally")

    # Show different matches
    print("\n4. MATCH DIFFERENCES")
    print("-" * 70)

    greedy_pairs = set(
        zip(
            greedy_matches["sponsor_name"].to_list(),
            greedy_matches["ticker"].to_list(),
        )
    )
    hungarian_pairs = set(
        zip(
            hungarian_matches["sponsor_name"].to_list(),
            hungarian_matches["ticker"].to_list(),
        )
    )

    only_greedy = greedy_pairs - hungarian_pairs
    only_hungarian = hungarian_pairs - greedy_pairs

    if only_greedy or only_hungarian:
        print(f"\nTotal different pairs: {len(only_greedy) + len(only_hungarian)}")
        print(f"  Only in greedy: {len(only_greedy)}")
        print(f"  Only in hungarian: {len(only_hungarian)}")

        if only_greedy:
            print(f"\nExample matches only in GREEDY (showing up to 5):")
            for i, (sponsor, ticker) in enumerate(list(only_greedy)[:5], 1):
                row = greedy_matches.filter(
                    (pl.col("sponsor_name") == sponsor) & (pl.col("ticker") == ticker)
                ).row(0, named=True)
                print(f"  {i}. {sponsor[:40]:40} → {ticker:6} ({row['confidence']:.3f})")

        if only_hungarian:
            print(f"\nExample matches only in HUNGARIAN (showing up to 5):")
            for i, (sponsor, ticker) in enumerate(list(only_hungarian)[:5], 1):
                row = hungarian_matches.filter(
                    (pl.col("sponsor_name") == sponsor) & (pl.col("ticker") == ticker)
                ).row(0, named=True)
                print(f"  {i}. {sponsor[:40]:40} → {ticker:6} ({row['confidence']:.3f})")
    else:
        print("\nNo differences - both algorithms produced identical matches!")

    # Save detailed results
    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "predictions_path": str(predictions_path),
        "total_pairs": len(all_pairs),
        "filtered_pairs": len(filtered_pairs),
        "min_threshold": min_threshold,
        "scoring_comparison": comparison,
        "evaluation": {
            "greedy": {
                "matches": len(greedy_matches),
                "precision": greedy_metrics.precision,
                "recall": greedy_metrics.recall,
                "f1": greedy_metrics.f1,
                "coverage": greedy_metrics.coverage,
                "true_positives": greedy_metrics.true_positives,
                "false_positives": greedy_metrics.false_positives,
                "false_negatives": greedy_metrics.false_negatives,
            },
            "hungarian": {
                "matches": len(hungarian_matches),
                "precision": hungarian_metrics.precision,
                "recall": hungarian_metrics.recall,
                "f1": hungarian_metrics.f1,
                "coverage": hungarian_metrics.coverage,
                "true_positives": hungarian_metrics.true_positives,
                "false_positives": hungarian_metrics.false_positives,
                "false_negatives": hungarian_metrics.false_negatives,
            },
            "improvements": {
                "f1": f1_improvement,
                "precision": precision_improvement,
                "recall": recall_improvement,
            },
        },
    }

    output_path = Path("data/matching_comparison.json")
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n📊 Detailed results saved to: {output_path}")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
