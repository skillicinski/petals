"""Evaluate entity resolution predictions against ground truth.

This script evaluates a predictions file (1:1 matched sponsor-ticker pairs)
against ground truth labels to measure precision, recall, and F1 score.

Usage:
    # Evaluate existing predictions
    uv run python scripts/evaluate_predictions.py

    # Evaluate with custom file
    uv run python scripts/evaluate_predictions.py --predictions data/my_predictions.parquet

The script expects:
- Predictions file with columns: sponsor_name, ticker, confidence
- Ground truth in: src/analytics/entity_resolution/data/ground_truth.csv
"""

import argparse
from pathlib import Path

from src.analytics.entity_resolution.evaluation import (
    load_ground_truth,
    generate_evaluation_report,
    compute_metrics,
)


def main():
    parser = argparse.ArgumentParser(description="Evaluate entity resolution predictions")
    parser.add_argument(
        "--predictions",
        default="data/candidates.parquet",
        help="Path to predictions file (parquet)",
    )
    parser.add_argument(
        "--output",
        default="data/evaluation_report.json",
        help="Path to save evaluation report (JSON)",
    )
    args = parser.parse_args()

    predictions_path = Path(args.predictions)
    if not predictions_path.exists():
        print(f"Error: {predictions_path} not found")
        print("\nRun the entity resolution pipeline first:")
        print("  AWS_PROFILE=personal uv run python -m src.analytics.entity_resolution.main")
        return

    print("=" * 70)
    print("ENTITY RESOLUTION EVALUATION")
    print("=" * 70)
    print(f"\nPredictions: {predictions_path}")
    print(f"Output: {args.output}")

    # Load data
    import polars as pl
    predictions = pl.read_parquet(predictions_path)
    ground_truth = load_ground_truth()

    print(f"\nPredictions: {len(predictions)} pairs")
    print(f"Ground truth: {len(ground_truth)} labels")

    # Check if predictions are 1:1
    n_sponsors = predictions["sponsor_name"].n_unique()
    n_tickers = predictions["ticker"].n_unique()
    is_1to1 = (n_sponsors == len(predictions)) and (n_tickers == len(predictions))

    print(f"\nUnique sponsors: {n_sponsors}")
    print(f"Unique tickers: {n_tickers}")
    print(f"1:1 matching: {'✓ Yes' if is_1to1 else '✗ No (many-to-many)'}")

    # Generate evaluation report
    print("\n" + "=" * 70)
    print("EVALUATION METRICS")
    print("=" * 70)

    report = generate_evaluation_report(
        predictions,
        ground_truth,
        output_path=args.output,
    )

    # Print summary
    overall = report["overall_metrics"]
    print(f"\nOverall Performance:")
    print(f"  Precision: {overall['precision']:.4f} ({overall['true_positives']}/{overall['true_positives'] + overall['false_positives']})")
    print(f"  Recall:    {overall['recall']:.4f} ({overall['true_positives']}/{overall['true_positives'] + overall['false_negatives']})")
    print(f"  F1 Score:  {overall['f1']:.4f}")
    print(f"  Coverage:  {overall['coverage']:.1%}")

    # Best threshold
    best = report["best_threshold"]
    print(f"\nOptimal Threshold: {best['threshold']:.2f}")
    print(f"  Precision: {best['precision']:.4f}")
    print(f"  Recall:    {best['recall']:.4f}")
    print(f"  F1 Score:  {best['f1']:.4f}")

    # Interpretation
    print("\n" + "=" * 70)
    print("INTERPRETATION")
    print("=" * 70)

    if overall['precision'] >= 0.85:
        print("✓ HIGH PRECISION: Very few false positives (reliable matches)")
    elif overall['precision'] >= 0.70:
        print("→ GOOD PRECISION: Reasonable false positive rate")
    else:
        print("✗ LOW PRECISION: Many false positives (needs improvement)")

    if overall['recall'] >= 0.90:
        print("✓ HIGH RECALL: Finding most correct matches")
    elif overall['recall'] >= 0.75:
        print("→ GOOD RECALL: Finding many correct matches")
    else:
        print("✗ LOW RECALL: Missing many correct matches")

    if overall['f1'] >= 0.80:
        print("✓ STRONG F1: Well-balanced performance")
    elif overall['f1'] >= 0.65:
        print("→ MODERATE F1: Reasonable overall performance")
    else:
        print("✗ WEAK F1: Needs improvement")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    main()
