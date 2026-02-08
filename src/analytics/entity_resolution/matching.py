"""Matching algorithms for entity resolution.

Provides different strategies for 1:1 entity matching:

1. **Greedy Matching** (current baseline)
   - Sort pairs by confidence descending
   - For each pair, assign if neither entity is already matched
   - Time: O(n²), Space: O(n)
   - Locally optimal but not globally optimal

2. **Hungarian Algorithm** (optimal)
   - Solves the assignment problem using linear_sum_assignment
   - Finds maximum weight matching in bipartite graph
   - Time: O(n³), Space: O(n²)
   - Guaranteed globally optimal solution

The Assignment Problem:
----------------------
Given a bipartite graph with:
- Left nodes: Sponsors (N entities)
- Right nodes: Tickers (M entities)
- Edge weights: Confidence scores

Find a 1:1 matching that maximizes total confidence.

Example where greedy fails:
--------------------------
Sponsors: [A, B]
Tickers: [X, Y]

Scores:
  A→X: 0.90, A→Y: 0.85
  B→X: 0.88, B→Y: 0.70

Greedy picks: A→X (0.90), B→Y (0.70) = 1.60 total
Optimal picks: A→Y (0.85), B→X (0.88) = 1.73 total

The greedy algorithm matches A→X first (highest score), but this blocks
B from getting its best match (B→X). The optimal solution swaps them.
"""

import polars as pl
import numpy as np
from scipy.optimize import linear_sum_assignment


def greedy_matching(
    pairs_df: pl.DataFrame,
    left_key: str = "sponsor_name",
    right_key: str = "ticker",
    score_key: str = "confidence",
    min_score: float = 0.0,
) -> pl.DataFrame:
    """Select 1:1 matches using greedy algorithm (baseline).

    Iterates through pairs sorted by score descending and selects each pair
    if neither entity is already matched.

    Args:
        pairs_df: All candidate pairs with scores
        left_key: Column name for left entities (sponsors)
        right_key: Column name for right entities (tickers)
        score_key: Column name for similarity scores
        min_score: Minimum score threshold

    Returns:
        DataFrame with selected 1:1 matches
    """
    df = pairs_df.filter(pl.col(score_key) >= min_score)

    if len(df) == 0:
        return df

    df = df.sort(score_key, descending=True)

    matched_left: set[str] = set()
    matched_right: set[str] = set()
    selected_rows = []

    for row in df.iter_rows(named=True):
        left_val = row[left_key]
        right_val = row[right_key]

        if left_val in matched_left or right_val in matched_right:
            continue

        matched_left.add(left_val)
        matched_right.add(right_val)
        selected_rows.append(row)

    if not selected_rows:
        return df.head(0)

    return pl.DataFrame(selected_rows)


def hungarian_matching(
    pairs_df: pl.DataFrame,
    left_key: str = "sponsor_name",
    right_key: str = "ticker",
    score_key: str = "confidence",
    min_score: float = 0.0,
) -> pl.DataFrame:
    """Select 1:1 matches using Hungarian algorithm (optimal).

    Solves the assignment problem to find the globally optimal matching.
    Uses scipy's linear_sum_assignment which implements the Hungarian algorithm.

    The algorithm:
    1. Build a cost matrix from similarity scores (negate for max matching)
    2. Find optimal assignment using Hungarian algorithm
    3. Filter out matches below threshold

    Args:
        pairs_df: All candidate pairs with scores
        left_key: Column name for left entities (sponsors)
        right_key: Column name for right entities (tickers)
        score_key: Column name for similarity scores
        min_score: Minimum score threshold

    Returns:
        DataFrame with optimal 1:1 matches
    """
    df = pairs_df.filter(pl.col(score_key) >= min_score)

    if len(df) == 0:
        return df

    # Get unique entities on each side
    left_entities = df[left_key].unique().sort().to_list()
    right_entities = df[right_key].unique().sort().to_list()

    n_left = len(left_entities)
    n_right = len(right_entities)

    # Build index mappings
    left_idx = {entity: i for i, entity in enumerate(left_entities)}
    right_idx = {entity: i for i, entity in enumerate(right_entities)}

    # Initialize cost matrix with very low scores (penalizes missing edges)
    # We use negative scores because linear_sum_assignment minimizes cost
    cost_matrix = np.full((n_left, n_right), fill_value=1e9)

    # Build lookup for all pair data
    pair_lookup: dict[tuple[str, str], dict] = {}
    for row in df.iter_rows(named=True):
        pair_lookup[(row[left_key], row[right_key])] = row

    # Fill cost matrix with negative scores (for maximization)
    for (left_ent, right_ent), row in pair_lookup.items():
        i = left_idx[left_ent]
        j = right_idx[right_ent]
        # Negate score because algorithm minimizes cost
        cost_matrix[i, j] = -row[score_key]

    # Run Hungarian algorithm
    left_indices, right_indices = linear_sum_assignment(cost_matrix)

    # Extract matched pairs
    selected_rows = []
    for i, j in zip(left_indices, right_indices):
        left_ent = left_entities[i]
        right_ent = right_entities[j]

        # Check if this pair exists in our candidates (not just a default assignment)
        pair_key = (left_ent, right_ent)
        if pair_key in pair_lookup:
            row = pair_lookup[pair_key]
            # Only include if score meets threshold
            if row[score_key] >= min_score:
                selected_rows.append(row)

    if not selected_rows:
        return df.head(0)

    result = pl.DataFrame(selected_rows)
    # Sort by score for consistency with greedy
    result = result.sort(score_key, descending=True)

    return result


def compare_matching_algorithms(
    pairs_df: pl.DataFrame,
    left_key: str = "sponsor_name",
    right_key: str = "ticker",
    score_key: str = "confidence",
    min_score: float = 0.0,
) -> dict:
    """Compare greedy and Hungarian matching algorithms.

    Useful for understanding when and how much the optimal algorithm improves
    over the greedy baseline.

    Args:
        pairs_df: All candidate pairs with scores
        left_key: Column name for left entities
        right_key: Column name for right entities
        score_key: Column name for similarity scores
        min_score: Minimum score threshold

    Returns:
        Dictionary with comparison results:
        - greedy_matches: Number of matches from greedy
        - greedy_total_score: Sum of confidence scores (greedy)
        - greedy_avg_score: Average confidence score (greedy)
        - hungarian_matches: Number of matches from Hungarian
        - hungarian_total_score: Sum of confidence scores (Hungarian)
        - hungarian_avg_score: Average confidence score (Hungarian)
        - improvement: Absolute improvement in total score
        - improvement_pct: Percentage improvement
        - different_pairs: Number of pairs that differ between methods
    """
    greedy_result = greedy_matching(pairs_df, left_key, right_key, score_key, min_score)
    hungarian_result = hungarian_matching(pairs_df, left_key, right_key, score_key, min_score)

    greedy_total = greedy_result[score_key].sum() if len(greedy_result) > 0 else 0.0
    greedy_avg = greedy_result[score_key].mean() if len(greedy_result) > 0 else 0.0

    hungarian_total = hungarian_result[score_key].sum() if len(hungarian_result) > 0 else 0.0
    hungarian_avg = hungarian_result[score_key].mean() if len(hungarian_result) > 0 else 0.0

    improvement = hungarian_total - greedy_total
    improvement_pct = (improvement / greedy_total * 100) if greedy_total > 0 else 0.0

    # Find pairs that differ
    greedy_pairs = set(
        zip(
            greedy_result[left_key].to_list(),
            greedy_result[right_key].to_list(),
        )
    )
    hungarian_pairs = set(
        zip(
            hungarian_result[left_key].to_list(),
            hungarian_result[right_key].to_list(),
        )
    )

    different_pairs = len(greedy_pairs.symmetric_difference(hungarian_pairs))

    return {
        "greedy_matches": len(greedy_result),
        "greedy_total_score": float(greedy_total),
        "greedy_avg_score": float(greedy_avg),
        "hungarian_matches": len(hungarian_result),
        "hungarian_total_score": float(hungarian_total),
        "hungarian_avg_score": float(hungarian_avg),
        "improvement": float(improvement),
        "improvement_pct": float(improvement_pct),
        "different_pairs": different_pairs,
    }
