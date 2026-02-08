# Entity Resolution - Ground Truth Data

This directory contains labeled evaluation data for measuring entity resolution quality.

## Dataset: ground_truth.csv

**Purpose:** Evaluate precision, recall, and F1 score of sponsor→ticker entity matching

**Size:** 52 manually labeled examples

**Balance:**
- 73% correct matches (38 labels)
- 27% incorrect matches (14 labels)
- 0% unknown (all resolved)

**Confidence Coverage:**
- High confidence (≥0.90): 22 labels
- Medium-high (0.80-0.90): 13 labels
- Medium (0.70-0.80): 13 labels
- Low (<0.70): 4 labels

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `sponsor_name` | string | Sponsor entity from clinical trials |
| `ticker` | string | Stock ticker symbol |
| `label` | string | Ground truth: "correct", "incorrect", or "unknown" |
| `confidence` | float | Pipeline's confidence score (0.0-1.0) |
| `notes` | string | Labeling rationale and context |

## Labeling Criteria

### "correct"
- Same company or direct subsidiary
- Acquired company (use acquirer's ticker)
- Common abbreviations (e.g., "3M" → MMM)

### "incorrect"
- Different companies entirely
- Wrong security type (e.g., warrant vs stock)
- Unrelated similar names

### "unknown"
- Insufficient information to decide
- Complex ownership structures

## Usage

```python
from src.analytics.entity_resolution.evaluation import load_ground_truth

# Load ground truth
df = load_ground_truth()  # Uses this file by default

# Or specify path explicitly
from src.analytics.entity_resolution.data import GROUND_TRUTH_PATH
df = load_ground_truth(str(GROUND_TRUTH_PATH))
```

## Version History

- **2026-02-08**: Initial dataset created (52 labels)
  - Labeled across 3 batches
  - Diverse confidence coverage
  - Good balance (73% correct)

## Adding More Labels

To expand this dataset:

1. Generate candidates:
   ```bash
   AWS_PROFILE=personal LIMIT_SPONSORS=N LIMIT_TICKERS=M \
     uv run python -m src.analytics.entity_resolution.main
   ```

2. Label using interactive tool:
   ```bash
   python scripts/label_helper.py
   ```

3. Copy new labels here:
   ```bash
   cp data/ground_truth/sponsor_ticker_labels.csv \
      src/analytics/entity_resolution/data/ground_truth.csv
   ```

4. Commit the updated dataset

## Quality Metrics

With this dataset, we can measure:
- **Precision**: % of predictions that are correct
- **Recall**: % of correct matches found
- **F1 Score**: Harmonic mean of precision and recall
- **Precision@K**: Quality of top K results
- **Threshold calibration**: Optimal confidence cutoffs
