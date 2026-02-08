# Phase 2: Optimal Bipartite Matching

## Summary

Implemented the Hungarian algorithm for optimal 1:1 entity matching as an alternative to the greedy baseline. Both algorithms are now available in the pipeline.

## What Was Built

### 1. Matching Algorithms Module (`matching.py`)

New module with three functions:
- `greedy_matching()` - O(n²) baseline algorithm
- `hungarian_matching()` - O(n³) optimal algorithm using scipy
- `compare_matching_algorithms()` - Compare both on same dataset

**Key ML Concept Learned: The Assignment Problem**
- Classic operations research problem
- Given bipartite graph with weighted edges
- Find maximum weight matching (1:1)
- Greedy: locally optimal, fast
- Hungarian: globally optimal, slower

### 2. Pipeline Integration

Updated `main.py` to support both algorithms:
```python
# Environment variable controls algorithm choice
MATCHING_ALGORITHM=hungarian  # or 'greedy' (default: hungarian)
```

Both algorithms now available in production pipeline.

### 3. Comprehensive Testing

Created `tests/test_matching.py` with 11 tests:
- Simple cases where algorithms agree
- Conflict cases where Hungarian outperforms greedy
- Edge cases (empty, single pair, threshold filtering)
- Algorithm comparison validation

**All tests passing ✓**

### 4. Evaluation Scripts

Two new evaluation scripts:
- `scripts/compare_matching_algorithms.py` - Compare greedy vs Hungarian
- `scripts/evaluate_predictions.py` - Evaluate any predictions file against ground truth

### 5. Documentation

Created `docs/matching_algorithms.md` explaining:
- How each algorithm works with examples
- Time/space complexity analysis
- When to use each algorithm
- Empirical results on our dataset
- Learning resources (graph theory, assignment problem)

## Empirical Results

Evaluated both algorithms on production dataset (1,130 sponsor-ticker pairs):

```
Greedy:     1,130 matches, total score: 935.33, avg: 0.828
Hungarian:  1,130 matches, total score: 935.33, avg: 0.828
Difference: 0 pairs (identical results)
```

**Key Finding:** Algorithms perform identically on our dataset. This indicates:
- Few conflicts in the matching space
- Token-based blocking effectively reduces competition
- Greedy happened to find globally optimal solution
- Dataset is sparse enough that local choices don't block better global solutions

**Implication:** Either algorithm works well for this use case, but Hungarian provides **guarantee** of optimality.

## What You Learned

### Graph Theory & Optimization
- **Bipartite graphs**: Two disjoint sets with edges between sets
- **Matching problem**: Selecting edges with no shared nodes
- **Maximum weight matching**: Finding best total score
- **Assignment problem**: Classic OR problem solved by Hungarian algorithm

### Greedy vs Optimal Algorithms
- **Greedy algorithms**: Make locally optimal choices at each step
  - Fast, simple, often work well
  - Not always globally optimal
  - Can get stuck in local optima

- **Optimal algorithms**: Guarantee best solution
  - Slower, more complex
  - Required when optimality is critical
  - Hungarian algorithm uses augmenting paths (graph theory)

### When Greedy Fails (Example)
```
Scores:
  A→X: 0.90, A→Y: 0.85
  B→X: 0.88, B→Y: 0.70

Greedy: A→X (0.90), B→Y (0.70) = 1.60
Optimal: A→Y (0.85), B→X (0.88) = 1.73 ✓
```

Greedy picks A→X first (highest score) but this blocks B from its best match.

### Applied ML Concepts
- **Algorithm selection**: Fast vs optimal tradeoffs
- **Empirical evaluation**: Testing algorithms on real data
- **Complexity analysis**: O(n²) vs O(n³)
- **Problem framing**: Entity resolution as assignment problem

## Technical Implementation

**scipy.optimize.linear_sum_assignment:**
- Implements Hungarian algorithm (Kuhn, 1955)
- Minimizes cost (we negate scores for maximization)
- Returns optimal row→column assignment
- Production-ready, well-tested implementation

**Cost Matrix Approach:**
```python
# Build n×m cost matrix
cost_matrix[i][j] = -similarity(sponsor_i, ticker_j)

# Find optimal assignment
row_ind, col_ind = linear_sum_assignment(cost_matrix)

# Extract matches
matches = [(sponsors[i], tickers[j]) for i, j in zip(row_ind, col_ind)]
```

## Evaluation Insights

**Incomplete Labeling Problem:**
- Ground truth: 52 labeled pairs
- Production predictions: 1,130 pairs
- Coverage: 92.3% overlap
- Unlabeled pairs counted as false positives
- This inflates FP rate (precision: 3.19% on full dataset)

**Lesson:** Evaluation quality depends on ground truth coverage. Our baseline (66.1% precision) used a smaller, more fully-labeled dataset.

## Files Changed

**New files:**
- `src/analytics/entity_resolution/matching.py` (254 lines)
- `tests/test_matching.py` (214 lines)
- `scripts/compare_matching_algorithms.py` (230 lines)
- `scripts/evaluate_predictions.py` (121 lines)
- `docs/matching_algorithms.md` (detailed explanation)
- `docs/phase2_summary.md` (this file)

**Modified files:**
- `src/analytics/entity_resolution/main.py`
  - Import matching functions
  - Add MATCHING_ALGORITHM env var
  - Update select_best_matches() to use new module
  - Default to Hungarian algorithm

## Next Steps (Phase 3+)

Potential improvements for future phases:

1. **Improved Blocking** - Reduce false negatives from token-based filter
2. **Better Embeddings** - Try domain-specific models (BioBERT, etc.)
3. **Ensemble Methods** - Combine multiple similarity signals
4. **Active Learning** - Strategically select which pairs to label next
5. **Confidence Calibration** - Better threshold selection

## Usage

```bash
# Run with Hungarian algorithm (default)
AWS_PROFILE=personal python -m src.analytics.entity_resolution.main

# Run with greedy algorithm
AWS_PROFILE=personal MATCHING_ALGORITHM=greedy \
  python -m src.analytics.entity_resolution.main

# Compare algorithms
python scripts/compare_matching_algorithms.py

# Evaluate predictions
python scripts/evaluate_predictions.py
```

## References

- Kuhn, H. W. (1955). "The Hungarian Method for the assignment problem"
- scipy.optimize.linear_sum_assignment documentation
- Wikipedia: Assignment Problem, Hungarian Algorithm
