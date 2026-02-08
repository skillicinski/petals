#!/usr/bin/env python3
"""Interactive labeling helper for entity resolution ground truth.

Usage:
    python scripts/label_helper.py

This script helps you efficiently label entity matches by:
1. Showing candidates one at a time
2. Opening research links in your browser
3. Recording your labels
4. Saving to ground truth CSV
"""

import sys
import webbrowser
from pathlib import Path

import polars as pl


def show_candidate(row: dict, index: int, total: int) -> None:
    """Display candidate match for review."""
    print("\n" + "=" * 80)
    print(f"CANDIDATE {index + 1} of {total}")
    print("=" * 80)
    print(f"\nSponsor:     {row['sponsor_name']}")
    print(f"Ticker:      {row['ticker']}")
    print(f"Ticker Name: {row['ticker_name']}")
    print(f"Market:      {row.get('market', 'N/A')}")
    print(f"Confidence:  {row['confidence']:.3f}")
    print(f"Status:      {row['status']}")


def get_research_links(sponsor: str, ticker: str) -> dict[str, str]:
    """Generate research URLs."""
    return {
        "yahoo": f"https://finance.yahoo.com/quote/{ticker}",
        "google_sponsor": f"https://www.google.com/search?q={sponsor.replace(' ', '+')}",
        "google_ticker": f"https://www.google.com/search?q={ticker}+stock",
    }


def get_label(row: dict) -> tuple[str, str]:
    """Prompt user for label."""
    while True:
        print("\n" + "-" * 80)
        print("Is this match CORRECT?")
        print("  [c] Correct    - Same company or direct subsidiary")
        print("  [i] Incorrect  - Different companies")
        print("  [u] Unknown    - Can't determine (need more info)")
        print("  [r] Research   - Open research links in browser")
        print("  [s] Skip       - Label later")
        print("  [q] Quit       - Save and exit")
        print("-" * 80)

        choice = input("Your choice: ").strip().lower()

        if choice == "c":
            notes = input("Notes (optional, press Enter to skip): ").strip()
            return "correct", notes or "Confirmed match"

        elif choice == "i":
            notes = input("Why incorrect? (helpful for learning): ").strip()
            return "incorrect", notes or "Different companies"

        elif choice == "u":
            notes = input("What's uncertain?: ").strip()
            return "unknown", notes or "Needs more research"

        elif choice == "r":
            links = get_research_links(row["sponsor_name"], row["ticker"])
            print("\nOpening research links...")
            webbrowser.open(links["yahoo"])
            webbrowser.open(links["google_sponsor"])
            print("(Links opened in browser. Return here to label.)")
            continue

        elif choice == "s":
            return "skip", ""

        elif choice == "q":
            return "quit", ""

        else:
            print("❌ Invalid choice. Please try again.")


def load_existing_labels(csv_path: Path) -> pl.DataFrame:
    """Load existing ground truth labels."""
    if not csv_path.exists():
        return pl.DataFrame(schema={"sponsor_name": str, "ticker": str, "label": str, "confidence": float, "notes": str})

    df = pl.read_csv(csv_path)
    # Ensure all required columns exist
    for col in ["sponsor_name", "ticker", "label"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    return df


def save_labels(df: pl.DataFrame, csv_path: Path) -> None:
    """Save labels to CSV."""
    # Ensure directory exists
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Write CSV
    df.write_csv(csv_path)
    print(f"\n✓ Saved {len(df)} labels to {csv_path}")


def main():
    """Run interactive labeling session."""
    # Paths
    candidates_path = Path("data/candidates_for_labeling.parquet")
    labels_path = Path("data/ground_truth/sponsor_ticker_labels.csv")

    # Load data
    if not candidates_path.exists():
        print(f"❌ Error: {candidates_path} not found.")
        print("Run the pipeline first to generate candidates:")
        print("  AWS_PROFILE=personal LIMIT_SPONSORS=100 LIMIT_TICKERS=300 \\")
        print("    OUTPUT_PATH=data/candidates_for_labeling.parquet \\")
        print("    uv run python -m src.analytics.entity_resolution.main")
        sys.exit(1)

    candidates = pl.read_parquet(candidates_path)
    existing_labels = load_existing_labels(labels_path)

    # Filter out already-labeled pairs
    if len(existing_labels) > 0:
        labeled_pairs = existing_labels.select(["sponsor_name", "ticker"])
        candidates = candidates.join(
            labeled_pairs,
            on=["sponsor_name", "ticker"],
            how="anti",  # Anti-join: keep rows NOT in labels
        )

    print("\n" + "=" * 80)
    print("ENTITY RESOLUTION - INTERACTIVE LABELING")
    print("=" * 80)
    print(f"\nExisting labels: {len(existing_labels)}")
    print(f"Remaining to label: {len(candidates)}")
    print("\nTip: Focus on diverse examples (high/medium/low confidence)")

    # Sort by confidence for strategic labeling
    candidates = candidates.sort("confidence", descending=True)

    # Session tracking
    new_labels = []
    session_stats = {"correct": 0, "incorrect": 0, "unknown": 0, "skipped": 0}

    # Label each candidate
    for i, row in enumerate(candidates.iter_rows(named=True)):
        show_candidate(row, i, len(candidates))

        label, notes = get_label(row)

        if label == "quit":
            print("\n👋 Quitting...")
            break

        if label == "skip":
            session_stats["skipped"] += 1
            continue

        # Record label
        new_labels.append(
            {
                "sponsor_name": row["sponsor_name"],
                "ticker": row["ticker"],
                "label": label,
                "confidence": row["confidence"],
                "notes": notes,
            }
        )
        session_stats[label] += 1

        # Show progress
        print(f"\n✓ Labeled as: {label}")
        print(f"Session: {session_stats['correct']} correct, {session_stats['incorrect']} incorrect, {session_stats['unknown']} unknown")

    # Save new labels
    if new_labels:
        new_df = pl.DataFrame(new_labels)

        # Merge with existing
        if len(existing_labels) > 0:
            combined = pl.concat([existing_labels, new_df])
        else:
            combined = new_df

        save_labels(combined, labels_path)

        # Summary
        print("\n" + "=" * 80)
        print("SESSION SUMMARY")
        print("=" * 80)
        print(f"New labels: {len(new_labels)}")
        print(f"  - Correct: {session_stats['correct']}")
        print(f"  - Incorrect: {session_stats['incorrect']}")
        print(f"  - Unknown: {session_stats['unknown']}")
        print(f"  - Skipped: {session_stats['skipped']}")
        print(f"\nTotal labels: {len(combined)}")

        # Balance check
        correct_pct = (combined["label"] == "correct").sum() / len(combined) * 100
        print(f"Dataset balance: {correct_pct:.1f}% correct")

        if correct_pct < 50:
            print("⚠️  Consider adding more correct examples")
        elif correct_pct > 80:
            print("⚠️  Consider adding more incorrect examples")

    else:
        print("\n❌ No labels recorded this session.")


if __name__ == "__main__":
    main()
