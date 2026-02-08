"""Tests for entity resolution pipeline (embedding-based)."""

import polars as pl
import pytest

from src.analytics.entity_resolution.blocking import build_token_index, tokenize
from src.analytics.entity_resolution.config import (
    COMMON_TOKENS,
    CONFIDENCE_AUTO_APPROVE,
    CONFIDENCE_AUTO_REJECT,
    STATUS_APPROVED,
    STATUS_PENDING,
    STATUS_REJECTED,
)


class TestTokenize:
    """Tests for token extraction."""

    def test_tokenize_basic(self):
        """Extract tokens from company name."""
        tokens = tokenize("Pfizer Inc.")
        assert "pfizer" in tokens
        # 'inc' should be filtered as common token
        assert "inc" not in tokens

    def test_tokenize_filters_common_tokens(self):
        """Common tokens are filtered out."""
        tokens = tokenize("Global Pharmaceuticals Ltd")
        assert "global" not in tokens
        assert "pharmaceuticals" not in tokens
        assert "ltd" not in tokens

    def test_tokenize_keeps_significant_tokens(self):
        """Significant tokens are kept."""
        tokens = tokenize("Merck Sharp & Dohme")
        assert "merck" in tokens
        assert "sharp" in tokens
        assert "dohme" in tokens

    def test_tokenize_empty_string(self):
        """Empty string returns empty set."""
        assert tokenize("") == set()

    def test_tokenize_short_tokens_filtered(self):
        """Very short tokens are filtered."""
        tokens = tokenize("AB Corp")
        # 'ab' is only 2 chars but MIN_TOKEN_LENGTH is 2, so it should be kept
        # 'corp' is a common token
        assert "ab" in tokens
        assert "corp" not in tokens


class TestBuildTokenIndex:
    """Tests for inverted index building."""

    def test_build_token_index(self):
        """Build inverted index from DataFrame."""
        df = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT", "GOOG"],
                "name": ["Apple Inc.", "Microsoft Corporation", "Alphabet Inc."],
            }
        )

        index = build_token_index(df, key_col="ticker", text_col="name")

        assert "apple" in index
        assert "AAPL" in index["apple"]

        assert "microsoft" in index
        assert "MSFT" in index["microsoft"]

        assert "alphabet" in index
        assert "GOOG" in index["alphabet"]

    def test_build_token_index_multiple_matches(self):
        """Token can map to multiple entities."""
        df = pl.DataFrame(
            {
                "ticker": ["AMGN", "BIIB"],
                "name": ["Amgen Biotech", "Biogen Biotech"],  # Both have 'biotech'
            }
        )

        index = build_token_index(df, key_col="ticker", text_col="name")

        # 'biotech' is a common token and should be filtered
        assert "biotech" not in index

        # But the unique tokens should work
        assert "amgen" in index
        assert "biogen" in index


class TestConfig:
    """Tests for configuration values."""

    def test_thresholds_ordered(self):
        """Confidence thresholds are properly ordered."""
        assert CONFIDENCE_AUTO_REJECT < CONFIDENCE_AUTO_APPROVE
        assert 0 < CONFIDENCE_AUTO_REJECT < 1
        assert 0 < CONFIDENCE_AUTO_APPROVE <= 1

    def test_common_tokens_lowercase(self):
        """All common tokens are lowercase."""
        for token in COMMON_TOKENS:
            assert token == token.lower(), f"Token '{token}' should be lowercase"

    def test_status_values(self):
        """Status values are correct strings."""
        assert STATUS_APPROVED == "approved"
        assert STATUS_PENDING == "pending"
        assert STATUS_REJECTED == "rejected"


class TestMainPipeline:
    """Integration tests for the main pipeline functions."""

    def test_select_best_matches_greedy(self):
        """Best matches uses greedy 1:1 assignment."""
        from src.analytics.entity_resolution.main import select_best_matches

        # Create candidates where sponsor A matches both T1 (0.9) and T2 (0.8)
        # and sponsor B matches both T1 (0.85) and T2 (0.7)
        # Greedy should assign: A→T1 (best), then B→T2 (T1 taken)
        df = pl.DataFrame(
            {
                "sponsor_name": ["A", "A", "B", "B"],
                "ticker": ["T1", "T2", "T1", "T2"],
                "confidence": [0.9, 0.8, 0.85, 0.7],
            }
        )

        result = select_best_matches(df, min_confidence=0.0, algorithm="greedy")

        assert len(result) == 2
        pairs = set(zip(result["sponsor_name"].to_list(), result["ticker"].to_list()))
        assert ("A", "T1") in pairs  # Best match overall
        assert ("B", "T2") in pairs  # B's only option after T1 taken

    def test_select_best_matches_respects_min_confidence(self):
        """Matches below min_confidence are excluded."""
        from src.analytics.entity_resolution.main import select_best_matches

        df = pl.DataFrame(
            {
                "sponsor_name": ["A", "B"],
                "ticker": ["T1", "T2"],
                "confidence": [0.9, 0.3],  # B is below threshold
            }
        )

        result = select_best_matches(df, min_confidence=0.5)

        assert len(result) == 1
        assert result["sponsor_name"][0] == "A"

    def test_generate_all_pairs(self):
        """Generate all pairs without blocking."""
        from src.analytics.entity_resolution.main import generate_all_pairs

        left_df = pl.DataFrame({"sponsor_name": ["A", "B"]})
        right_df = pl.DataFrame({"ticker": ["T1", "T2", "T3"]})

        pairs = generate_all_pairs(left_df, right_df)

        assert len(pairs) == 2  # Two sponsors
        assert pairs["A"] == {"T1", "T2", "T3"}
        assert pairs["B"] == {"T1", "T2", "T3"}


class TestEmbeddings:
    """Tests for embedding functions (require sentence-transformers)."""

    @pytest.fixture
    def model_available(self):
        """Check if sentence-transformers can be loaded."""
        import importlib.util

        if importlib.util.find_spec("sentence_transformers") is None:
            pytest.skip("sentence-transformers not installed")

    def test_compute_embeddings_shape(self, model_available):
        """Embeddings have correct shape."""
        from src.analytics.entity_resolution.main import compute_embeddings

        texts = ["Hello world", "Test text"]
        embeddings = compute_embeddings(texts, show_progress=False)

        assert embeddings.shape[0] == 2  # Two texts
        assert embeddings.shape[1] == 384  # all-MiniLM-L6-v2 dimension

    def test_compute_embeddings_normalized(self, model_available):
        """Embeddings are L2 normalized."""
        import numpy as np

        from src.analytics.entity_resolution.main import compute_embeddings

        texts = ["Test"]
        embeddings = compute_embeddings(texts, show_progress=False)

        # L2 norm should be ~1.0
        norm = np.linalg.norm(embeddings[0])
        assert abs(norm - 1.0) < 0.001

    def test_similar_texts_high_similarity(self, model_available):
        """Similar texts have high cosine similarity."""
        import numpy as np

        from src.analytics.entity_resolution.main import compute_embeddings

        texts = ["Pfizer Inc.", "Pfizer Corporation"]
        embeddings = compute_embeddings(texts, show_progress=False)

        # Cosine similarity (normalized, so just dot product)
        similarity = np.dot(embeddings[0], embeddings[1])

        assert similarity > 0.8  # Should be very similar

    def test_different_texts_lower_similarity(self, model_available):
        """Different texts have lower similarity."""
        import numpy as np

        from src.analytics.entity_resolution.main import compute_embeddings

        texts = ["Pfizer Inc.", "Apple Computer"]
        embeddings = compute_embeddings(texts, show_progress=False)

        similarity = np.dot(embeddings[0], embeddings[1])

        assert similarity < 0.5  # Should be quite different
