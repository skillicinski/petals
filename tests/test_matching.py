"""Tests for matching algorithms."""

import polars as pl
import pytest

from src.analytics.entity_resolution.matching import (
    greedy_matching,
    hungarian_matching,
    compare_matching_algorithms,
)


@pytest.fixture
def simple_pairs():
    """Simple test case where both algorithms agree."""
    return pl.DataFrame(
        {
            "sponsor_name": ["A", "A", "B", "B"],
            "ticker": ["X", "Y", "X", "Y"],
            "confidence": [0.9, 0.5, 0.4, 0.8],
        }
    )


@pytest.fixture
def conflict_pairs():
    """Test case where greedy and Hungarian differ.

    Greedy: A→X (0.90), B→Y (0.70) = 1.60
    Optimal: A→Y (0.85), B→X (0.88) = 1.73
    """
    return pl.DataFrame(
        {
            "sponsor_name": ["A", "A", "B", "B"],
            "ticker": ["X", "Y", "X", "Y"],
            "confidence": [0.90, 0.85, 0.88, 0.70],
        }
    )


def test_greedy_simple(simple_pairs):
    """Test greedy matching on simple case."""
    result = greedy_matching(simple_pairs)

    assert len(result) == 2
    assert set(result["sponsor_name"]) == {"A", "B"}
    assert set(result["ticker"]) == {"X", "Y"}

    # A should match X (0.9), B should match Y (0.8)
    a_match = result.filter(pl.col("sponsor_name") == "A").row(0, named=True)
    b_match = result.filter(pl.col("sponsor_name") == "B").row(0, named=True)

    assert a_match["ticker"] == "X"
    assert a_match["confidence"] == 0.9
    assert b_match["ticker"] == "Y"
    assert b_match["confidence"] == 0.8


def test_hungarian_simple(simple_pairs):
    """Test Hungarian matching on simple case."""
    result = hungarian_matching(simple_pairs)

    assert len(result) == 2
    assert set(result["sponsor_name"]) == {"A", "B"}
    assert set(result["ticker"]) == {"X", "Y"}

    # Should match same as greedy in this case
    a_match = result.filter(pl.col("sponsor_name") == "A").row(0, named=True)
    b_match = result.filter(pl.col("sponsor_name") == "B").row(0, named=True)

    assert a_match["ticker"] == "X"
    assert b_match["ticker"] == "Y"


def test_greedy_conflict(conflict_pairs):
    """Test greedy on case where it's suboptimal."""
    result = greedy_matching(conflict_pairs)

    assert len(result) == 2

    # Greedy picks A→X first (0.90), then B→Y (0.70)
    a_match = result.filter(pl.col("sponsor_name") == "A").row(0, named=True)
    b_match = result.filter(pl.col("sponsor_name") == "B").row(0, named=True)

    assert a_match["ticker"] == "X"
    assert a_match["confidence"] == 0.90
    assert b_match["ticker"] == "Y"
    assert b_match["confidence"] == 0.70

    total_score = result["confidence"].sum()
    assert total_score == pytest.approx(1.60)


def test_hungarian_conflict(conflict_pairs):
    """Test Hungarian finds optimal solution."""
    result = hungarian_matching(conflict_pairs)

    assert len(result) == 2

    # Hungarian finds optimal: A→Y (0.85), B→X (0.88)
    a_match = result.filter(pl.col("sponsor_name") == "A").row(0, named=True)
    b_match = result.filter(pl.col("sponsor_name") == "B").row(0, named=True)

    assert a_match["ticker"] == "Y"
    assert a_match["confidence"] == 0.85
    assert b_match["ticker"] == "X"
    assert b_match["confidence"] == 0.88

    total_score = result["confidence"].sum()
    assert total_score == pytest.approx(1.73)


def test_threshold_filtering():
    """Test that min_score threshold is respected."""
    pairs = pl.DataFrame(
        {
            "sponsor_name": ["A", "B", "C"],
            "ticker": ["X", "Y", "Z"],
            "confidence": [0.9, 0.5, 0.3],
        }
    )

    # With threshold 0.6, only A→X should match
    result = greedy_matching(pairs, min_score=0.6)
    assert len(result) == 1
    assert result["sponsor_name"][0] == "A"

    result = hungarian_matching(pairs, min_score=0.6)
    assert len(result) == 1
    assert result["sponsor_name"][0] == "A"


def test_empty_input():
    """Test handling of empty input."""
    empty = pl.DataFrame(
        schema={
            "sponsor_name": pl.Utf8,
            "ticker": pl.Utf8,
            "confidence": pl.Float64,
        }
    )

    result = greedy_matching(empty)
    assert len(result) == 0

    result = hungarian_matching(empty)
    assert len(result) == 0


def test_single_pair():
    """Test handling of single pair."""
    single = pl.DataFrame(
        {
            "sponsor_name": ["A"],
            "ticker": ["X"],
            "confidence": [0.9],
        }
    )

    result = greedy_matching(single)
    assert len(result) == 1

    result = hungarian_matching(single)
    assert len(result) == 1


def test_no_matches_above_threshold():
    """Test when all scores are below threshold."""
    pairs = pl.DataFrame(
        {
            "sponsor_name": ["A", "B"],
            "ticker": ["X", "Y"],
            "confidence": [0.3, 0.2],
        }
    )

    result = greedy_matching(pairs, min_score=0.5)
    assert len(result) == 0

    result = hungarian_matching(pairs, min_score=0.5)
    assert len(result) == 0


def test_compare_algorithms_simple(simple_pairs):
    """Test algorithm comparison on simple case."""
    comparison = compare_matching_algorithms(simple_pairs)

    assert comparison["greedy_matches"] == 2
    assert comparison["hungarian_matches"] == 2
    assert comparison["greedy_total_score"] == pytest.approx(1.7)
    assert comparison["hungarian_total_score"] == pytest.approx(1.7)
    assert comparison["improvement"] == pytest.approx(0.0)
    assert comparison["different_pairs"] == 0


def test_compare_algorithms_conflict(conflict_pairs):
    """Test algorithm comparison on conflict case."""
    comparison = compare_matching_algorithms(conflict_pairs)

    assert comparison["greedy_matches"] == 2
    assert comparison["hungarian_matches"] == 2
    assert comparison["greedy_total_score"] == pytest.approx(1.60)
    assert comparison["hungarian_total_score"] == pytest.approx(1.73)
    assert comparison["improvement"] == pytest.approx(0.13, abs=0.01)
    assert comparison["improvement_pct"] == pytest.approx(8.125, abs=0.1)
    assert comparison["different_pairs"] == 4  # All pairs differ


def test_many_to_one_candidates():
    """Test case with multiple sponsors competing for same ticker."""
    pairs = pl.DataFrame(
        {
            "sponsor_name": ["A", "B", "C", "A", "B", "C"],
            "ticker": ["X", "X", "X", "Y", "Y", "Z"],
            "confidence": [0.95, 0.90, 0.85, 0.70, 0.88, 0.92],
        }
    )

    greedy_result = greedy_matching(pairs)
    hungarian_result = hungarian_matching(pairs)

    # Both should produce 3 matches (1:1)
    assert len(greedy_result) == 3
    assert len(hungarian_result) == 3

    # Each sponsor and ticker should appear exactly once
    assert len(greedy_result["sponsor_name"].unique()) == 3
    assert len(greedy_result["ticker"].unique()) == 3
    assert len(hungarian_result["sponsor_name"].unique()) == 3
    assert len(hungarian_result["ticker"].unique()) == 3

    # Hungarian should find higher total score
    greedy_total = greedy_result["confidence"].sum()
    hungarian_total = hungarian_result["confidence"].sum()
    assert hungarian_total >= greedy_total
