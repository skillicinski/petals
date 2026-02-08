"""Enhanced labeling tool for entity resolution ground truth expansion.

This tool helps efficiently expand the ground truth dataset using strategic sampling
across confidence ranges to maximize learning value per label.

Features:
- Strategic sampling (high/medium/low confidence + missed matches)
- Shows enriched context (description, industry, sector)
- Pre-filters to unlabeled pairs only
- Auto-saves progress every 10 labels
- Statistics and progress tracking
- Research links for verification

Usage:
    # Interactive labeling with strategic sampling
    python scripts/label_helper_v2.py

    # Resume previous session
    python scripts/label_helper_v2.py --resume

    # Label specific confidence range
    python scripts/label_helper_v2.py --range medium

The tool will guide you through labeling with clear context and keyboard shortcuts.
"""

import argparse
import sys
import webbrowser
from pathlib import Path

import polars as pl

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.entity_resolution.evaluation import load_ground_truth


def load_predictions(path: str = "data/entity_matches.parquet") -> pl.DataFrame:
    """Load predictions from parquet file."""
    pred_path = Path(path)
    if not pred_path.exists():
        print(f"Error: {pred_path} not found")
        print("\nRun entity resolution first:")
        print("  AWS_PROFILE=personal python -m src.analytics.entity_resolution.main")
        sys.exit(1)

    return pl.read_parquet(pred_path)


def get_unlabeled_candidates(
    predictions: pl.DataFrame,
    ground_truth: pl.DataFrame,
) -> pl.DataFrame:
    """Filter predictions to only unlabeled pairs."""
    # Create set of already labeled pairs
    labeled_pairs = set(
        zip(
            ground_truth["sponsor_name"].to_list(),
            ground_truth["ticker"].to_list(),
        )
    )

    # Filter to unlabeled
    unlabeled = []
    for row in predictions.iter_rows(named=True):
        pair = (row["sponsor_name"], row["ticker"])
        if pair not in labeled_pairs:
            unlabeled.append(row)

    if not unlabeled:
        return predictions.head(0)

    return pl.DataFrame(unlabeled)


def strategic_sample(
    candidates: pl.DataFrame,
    target_counts: dict[str, int] | None = None,
) -> pl.DataFrame:
    """Sample candidates strategically across confidence ranges.

    Args:
        candidates: Unlabeled prediction pairs
        target_counts: Target number of samples per range
            Keys: 'high', 'medium', 'low'
            Default: {'high': 20, 'medium': 50, 'low': 50}

    Returns:
        Strategically sampled DataFrame
    """
    if target_counts is None:
        target_counts = {
            "high": 20,    # High confidence (≥0.85) - verify TPs
            "medium": 50,  # Medium (0.75-0.85) - borderline cases
            "low": 50,     # Low (0.65-0.75) - likely FPs
        }

    samples = []

    # High confidence (≥0.85)
    high = candidates.filter(pl.col("confidence") >= 0.85)
    if len(high) > 0:
        n = min(target_counts["high"], len(high))
        samples.append(high.sample(n=n, seed=42))
        print(f"Sampled {n} high-confidence pairs (≥0.85)")

    # Medium confidence (0.75-0.85)
    medium = candidates.filter(
        (pl.col("confidence") >= 0.75) & (pl.col("confidence") < 0.85)
    )
    if len(medium) > 0:
        n = min(target_counts["medium"], len(medium))
        samples.append(medium.sample(n=n, seed=42))
        print(f"Sampled {n} medium-confidence pairs (0.75-0.85)")

    # Low confidence (0.65-0.75)
    low = candidates.filter(
        (pl.col("confidence") >= 0.65) & (pl.col("confidence") < 0.75)
    )
    if len(low) > 0:
        n = min(target_counts["low"], len(low))
        samples.append(low.sample(n=n, seed=42))
        print(f"Sampled {n} low-confidence pairs (0.65-0.75)")

    if not samples:
        return candidates.head(0)

    # Combine and shuffle
    result = pl.concat(samples).sample(fraction=1.0, seed=42)
    return result


def save_labels(labels: list[dict], output_path: str = "data/ground_truth/sponsor_ticker_labels.csv"):
    """Save labels to CSV, appending to existing file if present."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    new_labels = pl.DataFrame(labels)

    # If file exists, append; otherwise create new
    if output.exists():
        existing = pl.read_csv(output)
        combined = pl.concat([existing, new_labels])
        # Remove duplicates (keep last)
        combined = combined.unique(subset=["sponsor_name", "ticker"], keep="last")
        combined.write_csv(output)
    else:
        new_labels.write_csv(output)

    print(f"\n💾 Saved {len(labels)} labels to {output}")


def show_candidate(row: dict, index: int, total: int):
    """Display candidate for labeling with enriched context."""
    print("\n" + "=" * 80)
    print(f"CANDIDATE {index + 1}/{total}")
    print("=" * 80)

    print(f"\n📊 Confidence: {row['confidence']:.3f}")
    print(f"Status: {row.get('status', 'N/A')}")

    print(f"\n📝 SPONSOR (Clinical Trial):")
    print(f"   {row['sponsor_name']}")

    print(f"\n🏢 TICKER (Public Company):")
    print(f"   Symbol: {row['ticker']}")
    print(f"   Name:   {row.get('ticker_name', 'N/A')}")

    # Show enriched context if available
    if row.get("description"):
        desc = row["description"]
        desc_preview = desc[:150] + "..." if len(desc) > 150 else desc
        print(f"   Description: {desc_preview}")

    if row.get("industry"):
        print(f"   Industry: {row['industry']}")

    if row.get("sector"):
        print(f"   Sector: {row['sector']}")

    print(f"\n❓ Is this the SAME entity?")
    print(f"   (Not just similar - must be the EXACT SAME company/organization)")


def label_interactive(candidates: pl.DataFrame, auto_save_every: int = 10):
    """Interactive labeling loop with auto-save."""
    labels = []
    total = len(candidates)

    print(f"\n🎯 Starting labeling session: {total} candidates")
    print("\nKeyboard shortcuts:")
    print("  [c] Correct   - Same entity (match)")
    print("  [i] Incorrect - Different entities (not a match)")
    print("  [u] Unknown   - Unsure, need more info")
    print("  [s] Skip      - Skip this one for now")
    print("  [r] Research  - Open Yahoo Finance + Google search")
    print("  [q] Quit      - Save and exit")

    for idx, row in enumerate(candidates.iter_rows(named=True)):
        show_candidate(row, idx, total)

        while True:
            choice = input("\n> ").strip().lower()

            if choice == "c":
                labels.append({
                    "sponsor_name": row["sponsor_name"],
                    "ticker": row["ticker"],
                    "label": "correct",
                    "confidence": row["confidence"],
                    "notes": "",
                })
                print("✓ Labeled as CORRECT")
                break

            elif choice == "i":
                notes = input("  Optional note (why incorrect?): ").strip()
                labels.append({
                    "sponsor_name": row["sponsor_name"],
                    "ticker": row["ticker"],
                    "label": "incorrect",
                    "confidence": row["confidence"],
                    "notes": notes,
                })
                print("✗ Labeled as INCORRECT")
                break

            elif choice == "u":
                notes = input("  Optional note (what's unclear?): ").strip()
                labels.append({
                    "sponsor_name": row["sponsor_name"],
                    "ticker": row["ticker"],
                    "label": "unknown",
                    "confidence": row["confidence"],
                    "notes": notes,
                })
                print("? Labeled as UNKNOWN")
                break

            elif choice == "s":
                print("⏭️  Skipped")
                break

            elif choice == "r":
                ticker = row["ticker"]
                sponsor = row["sponsor_name"]
                webbrowser.open(f"https://finance.yahoo.com/quote/{ticker}")
                webbrowser.open(f"https://www.google.com/search?q={sponsor.replace(' ', '+')}")
                print("🔍 Opened research links in browser")
                continue

            elif choice == "q":
                print("\n👋 Quitting...")
                if labels:
                    save_labels(labels)
                print(f"\nLabeled {len(labels)} candidates this session")
                return len(labels)

            else:
                print("❌ Invalid choice. Use: c/i/u/s/r/q")
                continue

        # Auto-save every N labels
        if labels and len(labels) % auto_save_every == 0:
            save_labels(labels)
            labels = []  # Clear after saving
            print(f"\n💾 Auto-saved progress ({idx + 1}/{total} completed)")

    # Save remaining labels
    if labels:
        save_labels(labels)

    print(f"\n✅ Completed! Labeled {total} candidates")
    return total


def show_statistics(predictions: pl.DataFrame, ground_truth: pl.DataFrame):
    """Show labeling progress and statistics."""
    total_preds = len(predictions)
    total_gt = len(ground_truth)

    # Count by label
    correct = (ground_truth["label"] == "correct").sum()
    incorrect = (ground_truth["label"] == "incorrect").sum()
    unknown = (ground_truth["label"] == "unknown").sum()

    # Coverage
    coverage = total_gt / total_preds * 100 if total_preds > 0 else 0

    print("\n" + "=" * 80)
    print("LABELING STATISTICS")
    print("=" * 80)
    print(f"\n📊 Progress:")
    print(f"   Total predictions: {total_preds}")
    print(f"   Labeled: {total_gt} ({coverage:.1f}% coverage)")
    print(f"   Unlabeled: {total_preds - total_gt}")

    print(f"\n📈 Label Distribution:")
    print(f"   Correct:   {correct} ({correct/total_gt*100:.1f}%)")
    print(f"   Incorrect: {incorrect} ({incorrect/total_gt*100:.1f}%)")
    print(f"   Unknown:   {unknown} ({unknown/total_gt*100:.1f}%)")

    print(f"\n🎯 Target: 200+ labels for effective fine-tuning")
    remaining = max(0, 200 - total_gt)
    if remaining > 0:
        print(f"   Remaining: {remaining} labels to reach goal")
    else:
        print(f"   ✅ Goal achieved! ({total_gt} labels)")


def main():
    parser = argparse.ArgumentParser(description="Enhanced entity resolution labeling tool")
    parser.add_argument(
        "--predictions",
        default="data/entity_matches.parquet",
        help="Predictions file to label",
    )
    parser.add_argument(
        "--range",
        choices=["high", "medium", "low", "all"],
        default="all",
        help="Confidence range to label (default: all, strategic sampling)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum candidates to show (default: strategic sample)",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("ENTITY RESOLUTION LABELING TOOL v2")
    print("=" * 80)

    # Load data
    predictions = load_predictions(args.predictions)
    ground_truth = load_ground_truth()

    # Show statistics
    show_statistics(predictions, ground_truth)

    # Get unlabeled candidates
    candidates = get_unlabeled_candidates(predictions, ground_truth)
    print(f"\n📋 Unlabeled candidates available: {len(candidates)}")

    if len(candidates) == 0:
        print("\n✨ All predictions are labeled! Consider:")
        print("  1. Run entity resolution on full dataset for more candidates")
        print("  2. Review and update existing labels")
        return

    # Strategic sampling or filter by range
    if args.range != "all":
        if args.range == "high":
            candidates = candidates.filter(pl.col("confidence") >= 0.85)
        elif args.range == "medium":
            candidates = candidates.filter(
                (pl.col("confidence") >= 0.75) & (pl.col("confidence") < 0.85)
            )
        elif args.range == "low":
            candidates = candidates.filter(
                (pl.col("confidence") >= 0.65) & (pl.col("confidence") < 0.75)
            )
        print(f"\nFiltered to {args.range} confidence range: {len(candidates)} candidates")
    else:
        # Strategic sampling
        print("\n🎯 Applying strategic sampling...")
        candidates = strategic_sample(candidates)

    # Apply limit if specified
    if args.limit:
        candidates = candidates.head(args.limit)
        print(f"Limited to {args.limit} candidates")

    if len(candidates) == 0:
        print("\nNo candidates in selected range.")
        return

    # Start labeling
    input(f"\n▶️  Press Enter to start labeling {len(candidates)} candidates...")
    labeled_count = label_interactive(candidates)

    # Final statistics
    ground_truth = load_ground_truth()  # Reload to include new labels
    show_statistics(predictions, ground_truth)

    print("\n" + "=" * 80)
    print("SESSION COMPLETE")
    print("=" * 80)
    print(f"\n✅ Labeled {labeled_count} new candidates")
    print(f"\n💡 Next steps:")
    print(f"   1. Review labels: data/ground_truth/sponsor_ticker_labels.csv")
    print(f"   2. Continue labeling: python scripts/label_helper_v2.py")
    print(f"   3. Evaluate: python scripts/evaluate_predictions.py")


if __name__ == "__main__":
    main()
