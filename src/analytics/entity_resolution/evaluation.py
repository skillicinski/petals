"""Evaluation framework for entity resolution.

Provides metrics to measure the quality of entity matching predictions
against ground truth labels.

Key Concepts:
-------------
- **Precision**: Of the matches we predicted, how many were correct?
  - High precision = few false positives (we don't match wrong entities)
  - Formula: TP / (TP + FP)

- **Recall**: Of the correct matches that exist, how many did we find?
  - High recall = few false negatives (we don't miss correct matches)
  - Formula: TP / (TP + FN)

- **F1 Score**: Harmonic mean of precision and recall
  - Balances precision and recall into single metric
  - Formula: 2 * (precision * recall) / (precision + recall)

- **Precision@K**: Precision considering only top K predictions
  - Useful for ranking evaluation (are top results high quality?)

- **Recall@K**: Recall considering only top K predictions
  - Measures how many correct matches are in top K results

Ground Truth Format:
-------------------
Expected columns:
- sponsor_name: str - The sponsor entity from clinical trials
- ticker: str - The ticker symbol being matched to
- label: str - One of: "correct", "incorrect", "unknown"

Predictions Format:
------------------
Expected columns:
- sponsor_name: str
- ticker: str
- confidence: float - Model's confidence score (0-1)
- status: str - One of: "approved", "pending", "rejected"
"""

from dataclasses import dataclass
from typing import Literal

import polars as pl


@dataclass
class EvaluationMetrics:
    """Container for evaluation metrics.

    Attributes:
        precision: TP / (TP + FP) - How many predictions were correct
        recall: TP / (TP + FN) - How many correct matches were found
        f1: Harmonic mean of precision and recall
        true_positives: Number of correct predictions
        false_positives: Number of incorrect predictions
        false_negatives: Number of missed correct matches
        true_negatives: Number of correct rejections (if applicable)
        coverage: % of entities that have at least one prediction
        total_predictions: Total number of predictions made
        total_ground_truth: Total number of ground truth labels
    """

    precision: float
    recall: float
    f1: float
    true_positives: int
    false_positives: int
    false_negatives: int
    true_negatives: int
    coverage: float
    total_predictions: int
    total_ground_truth: int

    def __str__(self) -> str:
        """Human-readable metrics summary."""
        return f"""
Evaluation Metrics:
  Precision: {self.precision:.3f} ({self.true_positives}/{self.true_positives + self.false_positives} predictions correct)
  Recall:    {self.recall:.3f} ({self.true_positives}/{self.true_positives + self.false_negatives} correct matches found)
  F1 Score:  {self.f1:.3f}
  Coverage:  {self.coverage:.1%} (entities with predictions)

Confusion Matrix:
  True Positives:  {self.true_positives}
  False Positives: {self.false_positives}
  False Negatives: {self.false_negatives}
  True Negatives:  {self.true_negatives}

Totals:
  Predictions:   {self.total_predictions}
  Ground Truth:  {self.total_ground_truth}
        """.strip()


def load_ground_truth(path: str | None = None) -> pl.DataFrame:
    """Load ground truth labels from CSV or Parquet.

    Args:
        path: Path to ground truth file. If None, uses the packaged dataset
              from src/analytics/entity_resolution/data/ground_truth.csv

    Returns:
        DataFrame with columns: sponsor_name, ticker, label

    Raises:
        ValueError: If required columns are missing
    """
    # Use packaged dataset by default
    if path is None:
        from .data import GROUND_TRUTH_PATH

        path = str(GROUND_TRUTH_PATH)

    if path.endswith(".csv"):
        df = pl.read_csv(path)
    elif path.endswith(".parquet"):
        df = pl.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file format: {path}")

    required_cols = {"sponsor_name", "ticker", "label"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing required columns. Expected: {required_cols}, Got: {df.columns}")

    # Validate labels
    valid_labels = {"correct", "incorrect", "unknown"}
    invalid = df.filter(~pl.col("label").is_in(valid_labels))
    if len(invalid) > 0:
        raise ValueError(f"Invalid labels found. Must be one of: {valid_labels}")

    return df


def compute_metrics(
    predictions: pl.DataFrame,
    ground_truth: pl.DataFrame,
    min_confidence: float = 0.0,
) -> EvaluationMetrics:
    """Compute evaluation metrics by comparing predictions to ground truth.

    Args:
        predictions: Predictions from model (sponsor_name, ticker, confidence)
        ground_truth: Ground truth labels (sponsor_name, ticker, label)
        min_confidence: Minimum confidence threshold for predictions

    Returns:
        EvaluationMetrics object with all computed metrics
    """
    # Filter predictions by confidence threshold
    preds = predictions.filter(pl.col("confidence") >= min_confidence)

    # Join predictions with ground truth
    joined = preds.join(
        ground_truth,
        on=["sponsor_name", "ticker"],
        how="left",
    )

    # Calculate confusion matrix components
    # TP: Predicted match that is labeled "correct"
    tp = (joined["label"] == "correct").sum()

    # FP: Predicted match that is labeled "incorrect"
    fp = (joined["label"] == "incorrect").sum()

    # FP also includes predictions with no ground truth label (assume incorrect)
    fp += joined["label"].is_null().sum()

    # FN: Ground truth "correct" that was not predicted
    correct_labels = ground_truth.filter(pl.col("label") == "correct")
    predicted_pairs = preds.select(["sponsor_name", "ticker"])

    fn_df = correct_labels.join(
        predicted_pairs,
        on=["sponsor_name", "ticker"],
        how="anti",  # Anti-join: rows in left that are NOT in right
    )
    fn = len(fn_df)

    # TN: Ground truth "incorrect" that was not predicted (harder to compute)
    # For entity resolution, we typically don't track all possible incorrect pairs
    # So TN is often not meaningful - we'll set it to 0
    tn = 0

    # Calculate metrics
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

    # Coverage: % of sponsors (from ground truth) that have at least one prediction
    sponsors_with_gt = ground_truth["sponsor_name"].unique()
    sponsors_with_pred = preds["sponsor_name"].unique()
    coverage = len(sponsors_with_pred.filter(sponsors_with_pred.is_in(sponsors_with_gt))) / len(
        sponsors_with_gt
    )

    return EvaluationMetrics(
        precision=precision,
        recall=recall,
        f1=f1,
        true_positives=tp,
        false_positives=fp,
        false_negatives=fn,
        true_negatives=tn,
        coverage=coverage,
        total_predictions=len(preds),
        total_ground_truth=len(ground_truth),
    )


def compute_metrics_at_k(
    predictions: pl.DataFrame,
    ground_truth: pl.DataFrame,
    k: int = 10,
) -> EvaluationMetrics:
    """Compute precision@K and recall@K.

    Useful for ranking evaluation - measures quality of top K predictions
    for each sponsor entity.

    Args:
        predictions: Predictions from model (must have confidence column)
        ground_truth: Ground truth labels
        k: Number of top predictions to consider per sponsor

    Returns:
        EvaluationMetrics computed on top-K predictions
    """
    # For each sponsor, keep only top K predictions by confidence
    top_k = predictions.sort("confidence", descending=True).group_by("sponsor_name").head(k)

    return compute_metrics(top_k, ground_truth, min_confidence=0.0)


def threshold_analysis(
    predictions: pl.DataFrame,
    ground_truth: pl.DataFrame,
    thresholds: list[float] | None = None,
) -> pl.DataFrame:
    """Analyze metrics across different confidence thresholds.

    Useful for:
    - Finding optimal confidence threshold
    - Understanding precision/recall tradeoffs
    - Generating ROC curves

    Args:
        predictions: Predictions from model
        ground_truth: Ground truth labels
        thresholds: List of thresholds to evaluate (default: 0.0 to 1.0 in steps of 0.05)

    Returns:
        DataFrame with columns: threshold, precision, recall, f1, tp, fp, fn
    """
    if thresholds is None:
        # Default: evaluate every 5% from 0% to 100%
        thresholds = [i / 20 for i in range(21)]  # 0.0, 0.05, 0.10, ..., 1.0

    results = []
    for threshold in thresholds:
        metrics = compute_metrics(predictions, ground_truth, min_confidence=threshold)
        results.append(
            {
                "threshold": threshold,
                "precision": metrics.precision,
                "recall": metrics.recall,
                "f1": metrics.f1,
                "true_positives": metrics.true_positives,
                "false_positives": metrics.false_positives,
                "false_negatives": metrics.false_negatives,
                "total_predictions": metrics.total_predictions,
            }
        )

    return pl.DataFrame(results)


def print_confusion_examples(
    predictions: pl.DataFrame,
    ground_truth: pl.DataFrame,
    category: Literal["tp", "fp", "fn"],
    limit: int = 10,
) -> None:
    """Print examples from confusion matrix categories.

    Useful for error analysis and understanding model behavior.

    Args:
        predictions: Predictions from model
        ground_truth: Ground truth labels
        category: Which category to show - "tp", "fp", or "fn"
        limit: Maximum number of examples to print
    """
    # Join predictions with ground truth
    joined = predictions.join(
        ground_truth,
        on=["sponsor_name", "ticker"],
        how="outer",
    )

    if category == "tp":
        # True Positives: predicted and labeled correct
        examples = joined.filter(
            pl.col("confidence").is_not_null() & (pl.col("label") == "correct")
        )
        print(f"\n=== TRUE POSITIVES (correct predictions) - Top {limit} ===")

    elif category == "fp":
        # False Positives: predicted but labeled incorrect (or no label)
        examples = joined.filter(
            pl.col("confidence").is_not_null()
            & ((pl.col("label") == "incorrect") | pl.col("label").is_null())
        )
        print(f"\n=== FALSE POSITIVES (incorrect predictions) - Top {limit} ===")

    elif category == "fn":
        # False Negatives: labeled correct but not predicted
        examples = joined.filter(pl.col("confidence").is_null() & (pl.col("label") == "correct"))
        print(f"\n=== FALSE NEGATIVES (missed correct matches) - Top {limit} ===")

    else:
        raise ValueError(f"Invalid category: {category}. Must be 'tp', 'fp', or 'fn'")

    # Sort by confidence (if available) and show top examples
    if "confidence" in examples.columns and category != "fn":
        examples = examples.sort("confidence", descending=True)

    for i, row in enumerate(examples.head(limit).iter_rows(named=True)):
        conf = f"{row.get('confidence', 0):.3f}" if row.get("confidence") else "N/A"
        label = row.get("label", "unlabeled")
        print(f"{i + 1}. [{conf}] {row['sponsor_name'][:50]:50} → {row['ticker']:6} ({label})")


def generate_evaluation_report(
    predictions: pl.DataFrame,
    ground_truth: pl.DataFrame,
    output_path: str | None = None,
) -> dict:
    """Generate comprehensive evaluation report.

    Args:
        predictions: Predictions from model
        ground_truth: Ground truth labels
        output_path: Optional path to save report as JSON

    Returns:
        Dictionary containing all evaluation results
    """
    import json

    # Compute overall metrics
    overall = compute_metrics(predictions, ground_truth)

    # Compute metrics at different K values
    metrics_at_k = {
        k: compute_metrics_at_k(predictions, ground_truth, k=k) for k in [1, 3, 5, 10, 20]
    }

    # Threshold analysis
    threshold_df = threshold_analysis(predictions, ground_truth)

    # Find best threshold by F1 score
    best_threshold_row = threshold_df.sort("f1", descending=True).head(1).row(0, named=True)

    report = {
        "overall_metrics": {
            "precision": overall.precision,
            "recall": overall.recall,
            "f1": overall.f1,
            "coverage": overall.coverage,
            "true_positives": overall.true_positives,
            "false_positives": overall.false_positives,
            "false_negatives": overall.false_negatives,
        },
        "metrics_at_k": {
            f"top_{k}": {
                "precision": m.precision,
                "recall": m.recall,
                "f1": m.f1,
            }
            for k, m in metrics_at_k.items()
        },
        "best_threshold": {
            "threshold": best_threshold_row["threshold"],
            "precision": best_threshold_row["precision"],
            "recall": best_threshold_row["recall"],
            "f1": best_threshold_row["f1"],
        },
        "threshold_analysis": threshold_df.to_dicts(),
    }

    # Save to file if requested
    if output_path:
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"[evaluation] Report saved to {output_path}")

    return report
