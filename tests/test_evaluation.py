"""Tests for entity resolution evaluation module."""

import polars as pl
import pytest

from src.analytics.entity_resolution.evaluation import (
    EvaluationMetrics,
    compute_metrics,
    compute_metrics_at_k,
    load_ground_truth,
    threshold_analysis,
)


class TestLoadGroundTruth:
    """Tests for ground truth loading."""

    def test_load_valid_csv(self, tmp_path):
        """Load valid ground truth CSV."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(
            "sponsor_name,ticker,label\n"
            '"Pfizer Inc",PFE,correct\n'
            '"Moderna Inc",MRNA,correct\n'
            '"Wrong Match",MSFT,incorrect\n'
        )

        df = load_ground_truth(str(csv_path))
        assert len(df) == 3
        assert set(df.columns) == {"sponsor_name", "ticker", "label"}

    def test_load_with_optional_columns(self, tmp_path):
        """Load ground truth with optional columns."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(
            "sponsor_name,ticker,label,confidence,notes\n"
            '"Pfizer Inc",PFE,correct,0.95,"Good match"\n'
        )

        df = load_ground_truth(str(csv_path))
        assert len(df) == 1
        assert "confidence" in df.columns
        assert "notes" in df.columns

    def test_missing_required_columns(self, tmp_path):
        """Raise error if required columns missing."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text('sponsor_name,ticker\n"Pfizer Inc",PFE\n')

        with pytest.raises(ValueError, match="Missing required columns"):
            load_ground_truth(str(csv_path))

    def test_invalid_label_values(self, tmp_path):
        """Raise error if invalid label values."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(
            'sponsor_name,ticker,label\n"Pfizer Inc",PFE,maybe\n'  # Invalid label
        )

        with pytest.raises(ValueError, match="Invalid labels found"):
            load_ground_truth(str(csv_path))


class TestComputeMetrics:
    """Tests for metrics computation."""

    @pytest.fixture
    def sample_ground_truth(self):
        """Sample ground truth data."""
        return pl.DataFrame(
            {
                "sponsor_name": ["A", "B", "C", "D"],
                "ticker": ["T1", "T2", "T3", "T4"],
                "label": ["correct", "correct", "incorrect", "correct"],
            }
        )

    @pytest.fixture
    def sample_predictions(self):
        """Sample prediction data."""
        return pl.DataFrame(
            {
                "sponsor_name": ["A", "B", "D", "E"],  # E is extra
                "ticker": ["T1", "T2", "T4", "T5"],
                "confidence": [0.9, 0.8, 0.7, 0.6],
            }
        )

    def test_perfect_prediction(self):
        """Perfect predictions: 100% precision and recall."""
        ground_truth = pl.DataFrame(
            {
                "sponsor_name": ["A", "B"],
                "ticker": ["T1", "T2"],
                "label": ["correct", "correct"],
            }
        )
        predictions = pl.DataFrame(
            {
                "sponsor_name": ["A", "B"],
                "ticker": ["T1", "T2"],
                "confidence": [0.9, 0.8],
            }
        )

        metrics = compute_metrics(predictions, ground_truth)
        assert metrics.precision == 1.0
        assert metrics.recall == 1.0
        assert metrics.f1 == 1.0
        assert metrics.true_positives == 2
        assert metrics.false_positives == 0
        assert metrics.false_negatives == 0

    def test_all_incorrect_predictions(self):
        """All predictions are incorrect: 0% precision."""
        ground_truth = pl.DataFrame(
            {
                "sponsor_name": ["A", "B"],
                "ticker": ["T1", "T2"],
                "label": ["correct", "correct"],
            }
        )
        predictions = pl.DataFrame(
            {
                "sponsor_name": ["A", "B"],
                "ticker": ["T1", "T2"],
                "confidence": [0.9, 0.8],
            }
        )
        # Mark them as incorrect in ground truth
        ground_truth = ground_truth.with_columns(pl.lit("incorrect").alias("label"))

        metrics = compute_metrics(predictions, ground_truth)
        assert metrics.precision == 0.0
        assert metrics.recall == 0.0  # No correct matches exist
        assert metrics.true_positives == 0
        assert metrics.false_positives == 2

    def test_missed_matches(self):
        """Some correct matches not predicted: recall < 1."""
        ground_truth = pl.DataFrame(
            {
                "sponsor_name": ["A", "B", "C"],
                "ticker": ["T1", "T2", "T3"],
                "label": ["correct", "correct", "correct"],
            }
        )
        predictions = pl.DataFrame(
            {
                "sponsor_name": ["A", "B"],  # Missing C
                "ticker": ["T1", "T2"],
                "confidence": [0.9, 0.8],
            }
        )

        metrics = compute_metrics(predictions, ground_truth)
        assert metrics.precision == 1.0  # Both predictions correct
        assert metrics.recall == 2 / 3  # Missed 1 out of 3
        assert metrics.true_positives == 2
        assert metrics.false_negatives == 1

    def test_mixed_results(self, sample_predictions, sample_ground_truth):
        """Mixed true/false positives and false negatives."""
        metrics = compute_metrics(sample_predictions, sample_ground_truth)

        # TP: A→T1, B→T2, D→T4 (3 correct matches)
        # FP: E→T5 (no ground truth for E)
        # FN: C→T3 not predicted (but labeled incorrect anyway, so not FN)
        #     Actually, since C is labeled "incorrect", it doesn't count as FN

        # Ground truth has 3 "correct" labels (A, B, D)
        # Predictions matched all 3 correctly
        assert metrics.true_positives == 3
        assert metrics.false_positives == 1  # E→T5
        assert metrics.false_negatives == 0  # All correct matches found

    def test_confidence_threshold(self):
        """Metrics respect confidence threshold."""
        ground_truth = pl.DataFrame(
            {
                "sponsor_name": ["A", "B"],
                "ticker": ["T1", "T2"],
                "label": ["correct", "correct"],
            }
        )
        predictions = pl.DataFrame(
            {
                "sponsor_name": ["A", "B"],
                "ticker": ["T1", "T2"],
                "confidence": [0.9, 0.5],  # B is below threshold
            }
        )

        metrics = compute_metrics(predictions, ground_truth, min_confidence=0.7)

        # Only A passes threshold
        assert metrics.true_positives == 1
        assert metrics.false_negatives == 1  # B not predicted
        assert metrics.recall == 0.5


class TestMetricsAtK:
    """Tests for precision@K and recall@K."""

    def test_metrics_at_k(self):
        """Compute metrics considering only top K per sponsor."""
        ground_truth = pl.DataFrame(
            {
                "sponsor_name": ["A", "A"],
                "ticker": ["T1", "T2"],
                "label": ["correct", "incorrect"],
            }
        )
        predictions = pl.DataFrame(
            {
                "sponsor_name": ["A", "A", "A"],
                "ticker": ["T2", "T1", "T3"],  # Wrong order
                "confidence": [0.9, 0.8, 0.7],
            }
        )

        # Top-1: Should only consider T2 (highest confidence)
        metrics_top1 = compute_metrics_at_k(predictions, ground_truth, k=1)
        assert metrics_top1.total_predictions == 1
        # T2 is labeled incorrect, so FP
        assert metrics_top1.false_positives == 1

        # Top-2: Should consider T2 and T1
        metrics_top2 = compute_metrics_at_k(predictions, ground_truth, k=2)
        assert metrics_top2.total_predictions == 2
        assert metrics_top2.true_positives == 1  # T1 is correct


class TestThresholdAnalysis:
    """Tests for threshold analysis."""

    def test_threshold_analysis(self):
        """Analyze metrics across thresholds."""
        ground_truth = pl.DataFrame(
            {
                "sponsor_name": ["A", "B", "C"],
                "ticker": ["T1", "T2", "T3"],
                "label": ["correct", "correct", "correct"],
            }
        )
        predictions = pl.DataFrame(
            {
                "sponsor_name": ["A", "B", "C"],
                "ticker": ["T1", "T2", "T3"],
                "confidence": [0.9, 0.75, 0.4],
            }
        )

        df = threshold_analysis(predictions, ground_truth, thresholds=[0.0, 0.5, 0.8])

        assert len(df) == 3
        assert "threshold" in df.columns
        assert "precision" in df.columns
        assert "recall" in df.columns
        assert "f1" in df.columns

        # At threshold 0.0: all 3 predictions included
        row_0 = df.filter(pl.col("threshold") == 0.0).row(0, named=True)
        assert row_0["true_positives"] == 3
        assert row_0["precision"] == 1.0
        assert row_0["recall"] == 1.0

        # At threshold 0.5: only A and B included
        row_05 = df.filter(pl.col("threshold") == 0.5).row(0, named=True)
        assert row_05["true_positives"] == 2
        assert row_05["recall"] == 2 / 3

        # At threshold 0.8: only A included
        row_08 = df.filter(pl.col("threshold") == 0.8).row(0, named=True)
        assert row_08["true_positives"] == 1
        assert row_08["recall"] == 1 / 3


class TestEvaluationMetrics:
    """Tests for EvaluationMetrics dataclass."""

    def test_metrics_str_representation(self):
        """String representation is human-readable."""
        metrics = EvaluationMetrics(
            precision=0.9,
            recall=0.8,
            f1=0.847,
            true_positives=9,
            false_positives=1,
            false_negatives=2,
            true_negatives=0,
            coverage=0.95,
            total_predictions=10,
            total_ground_truth=11,
        )

        s = str(metrics)
        assert "Precision: 0.900" in s
        assert "Recall:    0.800" in s
        assert "F1 Score:  0.847" in s
        assert "Coverage:  95.0%" in s
        assert "True Positives:  9" in s
