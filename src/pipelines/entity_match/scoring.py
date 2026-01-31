"""Score candidate pairs for entity matching.

Combines multiple signals:
- Alias set overlap (Jaccard similarity)
- Fuzzy token matching (rapidfuzz)
"""

import polars as pl
from rapidfuzz import fuzz

from .config import (
    CONFIDENCE_AUTO_APPROVE,
    CONFIDENCE_AUTO_REJECT,
    CONFIDENCE_REVIEW_HIGH,
    CONFIDENCE_REVIEW_LOW,
    STATUS_APPROVED,
    STATUS_PENDING,
    STATUS_REJECTED,
)


def jaccard_similarity(set_a: set[str], set_b: set[str]) -> float:
    """Calculate Jaccard similarity between two sets."""
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


def best_fuzzy_match(aliases_a: list[str], aliases_b: list[str]) -> tuple[float, str, str]:
    """Find best fuzzy match between two alias lists.

    Returns:
        (score, best_a, best_b) - best match score and the matching aliases
    """
    if not aliases_a or not aliases_b:
        return 0.0, '', ''

    best_score = 0.0
    best_a = ''
    best_b = ''

    for a in aliases_a:
        for b in aliases_b:
            # Use token_sort_ratio for word order independence
            score = fuzz.token_sort_ratio(a, b) / 100.0
            if score > best_score:
                best_score = score
                best_a = a
                best_b = b

    return best_score, best_a, best_b


def find_overlapping_aliases(aliases_a: list[str], aliases_b: list[str]) -> list[str]:
    """Find aliases that appear in both lists (exact match after normalization)."""
    set_a = set(a.lower().strip() for a in aliases_a)
    set_b = set(b.lower().strip() for b in aliases_b)
    return sorted(set_a & set_b)


def score_candidate(
    sponsor_aliases: list[str],
    ticker_aliases: list[str],
) -> dict:
    """Score a single candidate pair.

    Returns dict with:
        - confidence: float 0-1
        - match_reason: str explaining the match
        - status: 'pending' | 'approved' | 'rejected'
        - approved_by: 'machine' | None
        - rejected_by: 'machine' | None
    """
    # Normalize aliases
    sponsor_set = set(a.lower().strip() for a in sponsor_aliases)
    ticker_set = set(a.lower().strip() for a in ticker_aliases)

    # Calculate Jaccard similarity
    jaccard = jaccard_similarity(sponsor_set, ticker_set)

    # Find best fuzzy match
    fuzzy_score, fuzzy_a, fuzzy_b = best_fuzzy_match(
        list(sponsor_set), list(ticker_set)
    )

    # Find exact overlaps
    overlaps = find_overlapping_aliases(sponsor_aliases, ticker_aliases)

    # Combine scores: weight Jaccard and fuzzy equally
    # Boost if there are exact overlaps
    base_score = (jaccard + fuzzy_score) / 2
    overlap_boost = min(len(overlaps) * 0.1, 0.2)  # Up to 0.2 boost for overlaps
    confidence = min(base_score + overlap_boost, 1.0)

    # Build match reason
    reasons = []
    if overlaps:
        reasons.append(f'exact: {", ".join(overlaps[:3])}')
    if fuzzy_score > 0.7:
        reasons.append(f'fuzzy: "{fuzzy_a}" ~ "{fuzzy_b}" ({fuzzy_score:.0%})')

    match_reason = '; '.join(reasons) if reasons else 'token overlap only'

    # Determine status and auto-decisions based on confidence
    if confidence >= CONFIDENCE_AUTO_APPROVE:
        status = STATUS_APPROVED
        approved_by = 'machine'
        rejected_by = None
    elif confidence < CONFIDENCE_AUTO_REJECT:
        status = STATUS_REJECTED
        approved_by = None
        rejected_by = 'machine'
    else:
        # In review band - pending human review
        status = STATUS_PENDING
        approved_by = None
        rejected_by = None

    return {
        'confidence': round(confidence, 3),
        'match_reason': match_reason,
        'status': status,
        'approved_by': approved_by,
        'rejected_by': rejected_by,
    }


def score_candidates(candidates_df: pl.DataFrame) -> pl.DataFrame:
    """Score all candidate pairs.

    Expects columns: sponsor_aliases, ticker_aliases
    Adds columns: confidence, match_reason, status, approved_by, rejected_by
    """
    scores = []

    for row in candidates_df.iter_rows(named=True):
        sponsor_aliases = row.get('sponsor_aliases', [])
        ticker_aliases = row.get('ticker_aliases', [])

        score_result = score_candidate(sponsor_aliases, ticker_aliases)
        scores.append(score_result)

    # Build score columns
    scores_df = pl.DataFrame(scores)

    # Concatenate horizontally
    result = pl.concat([candidates_df, scores_df], how='horizontal')

    # Sort by confidence descending
    result = result.sort('confidence', descending=True)

    # Stats
    auto_approved = (result['status'] == STATUS_APPROVED).sum()
    pending = (result['status'] == STATUS_PENDING).sum()
    auto_rejected = (result['status'] == STATUS_REJECTED).sum()
    print(f'[scoring] Scored {len(result)} candidates: '
          f'{auto_approved} auto-approved, {pending} pending review, {auto_rejected} auto-rejected')

    return result
