### Matching Algorithms: Greedy vs Hungarian

This document explains the two 1:1 matching algorithms available in the entity resolution pipeline and when to use each.

## Overview

Both algorithms solve the same problem: Given N sponsors and M tickers with similarity scores, find the optimal 1:1 matching (each sponsor matched to at most one ticker, each ticker matched to at most one sponsor).

| Algorithm | Time | Space | Optimality | When to Use |
|-----------|------|-------|------------|-------------|
| **Greedy** | O(n²) | O(n) | Locally optimal | Fast, works well when few conflicts |
| **Hungarian** | O(n³) | O(n²) | Globally optimal | Guaranteed best solution, important decisions |

## Greedy Matching

**How it works:**
1. Sort all pairs by similarity score (descending)
2. Iterate through pairs in order
3. Select each pair if neither entity is already matched
4. Stop when all entities are matched or no valid pairs remain

**Example:**
```
Scores:
  A→X: 0.90, A→Y: 0.85
  B→X: 0.88, B→Y: 0.70

Greedy picks:
  1. A→X (0.90) ✓ highest score
  2. A→Y (0.85) ✗ A already matched
  3. B→X (0.88) ✗ X already matched
  4. B→Y (0.70) ✓ only option left

Result: A→X, B→Y
Total: 0.90 + 0.70 = 1.60
```

**Pros:**
- Fast O(n²) runtime
- Simple to understand
- Often finds optimal solution when conflicts are rare
- Low memory usage

**Cons:**
- Not guaranteed to be optimal
- Can make early decisions that block better overall solutions

## Hungarian Algorithm

**How it works:**
1. Build a cost matrix (negated similarity scores)
2. Find the minimum cost assignment using Hungarian algorithm
3. Return the optimal matching

The Hungarian algorithm uses graph theory (augmenting paths in bipartite graphs) to find the maximum weight matching in polynomial time.

**Same example:**
```
Scores:
  A→X: 0.90, A→Y: 0.85
  B→X: 0.88, B→Y: 0.70

Hungarian finds:
  A→Y (0.85)
  B→X (0.88)

Result: A→Y, B→X
Total: 0.85 + 0.88 = 1.73 ✓ optimal
```

**Pros:**
- Guaranteed globally optimal solution
- Better when many conflicts exist
- Mathematically proven correctness

**Cons:**
- Slower O(n³) runtime
- Higher memory O(n²) for cost matrix
- More complex implementation

## When Do They Differ?

The algorithms produce different results when there are **conflicts** - multiple entities competing for the same match.

**Low conflict scenario** (algorithms agree):
- Each sponsor has one clear best match
- Matches don't compete for same tickers
- Greedy happens to find optimal solution

**High conflict scenario** (algorithms differ):
- Multiple sponsors want the same ticker
- Early greedy decisions block better overall solutions
- Hungarian finds better total score

## Empirical Results

On our clinical trial entity resolution dataset (1,130 sponsor-ticker pairs):

```
Greedy:     1,130 matches, total score: 935.33, avg: 0.828
Hungarian:  1,130 matches, total score: 935.33, avg: 0.828
Difference: 0 pairs differ
```

**Interpretation:** No conflicts detected. This suggests:
- Most sponsors have clear best matches
- Token-based blocking (pre-filtering) reduces conflicts
- Dataset is sparse enough that conflicts are rare

## Recommendations

**Use Greedy when:**
- Speed is important
- Dataset has few conflicts (low competition)
- Running frequently in production
- Results show greedy ≈ Hungarian in testing

**Use Hungarian when:**
- Optimality is critical (e.g., final production matches)
- High-stakes decisions (e.g., financial or regulatory)
- Dataset has many conflicts
- Want guaranteed best solution

**For this project:**
- **Default: Hungarian** - Slightly slower but guaranteed optimal
- Runtime difference is negligible for typical dataset sizes
- Peace of mind that we have the best possible matching

## Configuration

Set the algorithm via environment variable:

```bash
# Use Hungarian (default)
MATCHING_ALGORITHM=hungarian uv run python -m src.analytics.entity_resolution.main

# Use Greedy
MATCHING_ALGORITHM=greedy uv run python -m src.analytics.entity_resolution.main
```

## Implementation

See `src/analytics/entity_resolution/matching.py` for:
- `greedy_matching()` - Fast greedy algorithm
- `hungarian_matching()` - Optimal Hungarian algorithm
- `compare_matching_algorithms()` - Compare both on same data

Tests: `tests/test_matching.py`

## Learning Resources

**The Assignment Problem:**
- Classic operations research problem
- Minimize cost of assigning workers to jobs
- Solved optimally by Hungarian algorithm (Kuhn, 1955)

**Graph Theory:**
- Bipartite graph: Two sets of nodes, edges between sets only
- Matching: Subset of edges with no shared nodes
- Maximum weight matching: Matching with highest sum of edge weights

**Key Insight:**
Greedy algorithms make locally optimal choices at each step. Sometimes this leads to a globally optimal solution (e.g., Dijkstra's shortest path). But for the assignment problem, greedy can get stuck in local optima. The Hungarian algorithm uses a more sophisticated approach (augmenting paths) to guarantee the global optimum.

## References

- Kuhn, H. W. (1955). "The Hungarian Method for the assignment problem". *Naval Research Logistics Quarterly*
- scipy.optimize.linear_sum_assignment documentation
- [Wikipedia: Assignment Problem](https://en.wikipedia.org/wiki/Assignment_problem)
- [Wikipedia: Hungarian Algorithm](https://en.wikipedia.org/wiki/Hungarian_algorithm)
