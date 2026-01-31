"""Blocking strategy for candidate pair generation.

Reduces O(n×m) comparisons to O(n×k) by only comparing entities
that share at least one significant token.
"""

import polars as pl

from .config import COMMON_TOKENS, MIN_TOKEN_LENGTH


def tokenize(text: str) -> set[str]:
    """Extract significant tokens from text for blocking.

    - Lowercase
    - Split on non-alphanumeric
    - Filter short tokens and common words
    """
    if not text:
        return set()

    # Split on non-alphanumeric characters
    tokens = set()
    current = []
    for char in text.lower():
        if char.isalnum():
            current.append(char)
        elif current:
            token = ''.join(current)
            if len(token) >= MIN_TOKEN_LENGTH and token not in COMMON_TOKENS:
                tokens.add(token)
            current = []

    # Don't forget last token
    if current:
        token = ''.join(current)
        if len(token) >= MIN_TOKEN_LENGTH and token not in COMMON_TOKENS:
            tokens.add(token)

    return tokens


def build_token_index(df: pl.DataFrame, key_col: str, text_col: str) -> dict[str, set[str]]:
    """Build inverted index: token -> set of keys.

    Args:
        df: DataFrame with entities
        key_col: Column to use as entity key
        text_col: Column to tokenize

    Returns:
        Dict mapping token -> set of entity keys
    """
    index: dict[str, set[str]] = {}

    for row in df.iter_rows(named=True):
        key = row[key_col]
        text = row[text_col]
        tokens = tokenize(text)

        for token in tokens:
            if token not in index:
                index[token] = set()
            index[token].add(key)

    return index


def generate_candidates(
    left_df: pl.DataFrame,
    right_df: pl.DataFrame,
    left_key: str = 'sponsor_name',
    right_key: str = 'ticker',
    right_text: str = 'name',
) -> pl.DataFrame:
    """Generate candidate pairs via token overlap blocking.

    Args:
        left_df: Left side entities (sponsors) with 'aliases' column
        right_df: Right side entities (tickers) with 'aliases' column
        left_key: Column name for left entity key
        right_key: Column name for right entity key
        right_text: Column in right_df to tokenize for blocking

    Returns:
        DataFrame with candidate pairs: left_key, right entity columns
    """
    # Build token index from right side (tickers)
    # We index on the 'name' column for initial blocking
    right_index = build_token_index(right_df, right_key, right_text)

    # For each left entity, find candidates via shared tokens
    candidates = []

    for row in left_df.iter_rows(named=True):
        left_entity = row[left_key]
        left_aliases = row.get('aliases', [])

        # Collect tokens from all aliases
        all_tokens: set[str] = set()
        for alias in left_aliases:
            all_tokens.update(tokenize(alias))

        # Find right-side candidates that share any token
        candidate_keys: set[str] = set()
        for token in all_tokens:
            if token in right_index:
                candidate_keys.update(right_index[token])

        # Record candidate pairs
        for right_ticker in candidate_keys:
            candidates.append({
                'sponsor_name': left_entity,
                'ticker': right_ticker,
            })

    if not candidates:
        print('[blocking] Warning: no candidate pairs generated')
        return pl.DataFrame(schema={
            'sponsor_name': pl.Utf8,
            'ticker': pl.Utf8,
        })

    candidates_df = pl.DataFrame(candidates)

    # Join to get full right-side data
    candidates_df = candidates_df.join(
        right_df,
        on='ticker',
        how='left'
    )

    # Join to get left-side aliases
    left_aliases_df = left_df.select([left_key, 'aliases']).rename({'aliases': 'sponsor_aliases'})
    candidates_df = candidates_df.join(
        left_aliases_df,
        left_on='sponsor_name',
        right_on=left_key,
        how='left'
    )

    # Rename right aliases
    candidates_df = candidates_df.rename({'aliases': 'ticker_aliases'})

    print(f'[blocking] Generated {len(candidates_df)} candidate pairs from {len(left_df)} sponsors × {len(right_df)} tickers')
    return candidates_df
