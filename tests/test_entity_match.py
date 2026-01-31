"""Tests for entity matching pipeline."""

import polars as pl
import pytest

from src.pipelines.entity_match.aliases import parse_aliases_response
from src.pipelines.entity_match.scoring import score_candidate, score_candidates


class TestParseAliasesResponse:
    """Tests for LLM response parsing."""

    def test_parse_clean_json_array(self):
        """Parse a clean JSON array of strings."""
        response = '["GSK", "GlaxoSmithKline", "Glaxo"]'
        result = parse_aliases_response(response)
        assert result == ["GSK", "GlaxoSmithKline", "Glaxo"]

    def test_parse_with_surrounding_text(self):
        """Extract JSON array embedded in other text."""
        response = 'Here are the aliases: ["GSK", "Glaxo"] Hope this helps!'
        result = parse_aliases_response(response)
        assert result == ["GSK", "Glaxo"]

    def test_parse_single_quotes(self):
        """Handle single-quoted JSON (common LLM error)."""
        response = "['GSK', 'Glaxo']"
        result = parse_aliases_response(response)
        assert result == ["GSK", "Glaxo"]

    def test_parse_invalid_object_syntax(self):
        """Handle {"GSK"} instead of "GSK" (common LLM error)."""
        response = '[{"GSK"}, "Glaxo", {"SmithKline"}]'
        result = parse_aliases_response(response)
        assert "GSK" in result
        assert "Glaxo" in result
        assert "SmithKline" in result

    def test_parse_nested_objects(self):
        """Flatten nested objects like {'alias': 'GSK'}."""
        response = '[{"alias": "GSK"}, {"ticker": "NVS"}, "Novartis"]'
        result = parse_aliases_response(response)
        assert "GSK" in result
        assert "NVS" in result
        assert "Novartis" in result

    def test_parse_empty_response(self):
        """Return empty list for empty response."""
        assert parse_aliases_response("") == []
        assert parse_aliases_response("No aliases found.") == []

    def test_parse_empty_array(self):
        """Parse empty JSON array."""
        assert parse_aliases_response("[]") == []

    def test_parse_invalid_json(self):
        """Return empty list for unparseable JSON."""
        response = "[GSK, Glaxo"  # Missing quotes and bracket
        result = parse_aliases_response(response)
        assert result == []


class TestScoring:
    """Tests for candidate scoring and schema consistency."""

    def test_score_candidate_high_confidence_approved(self):
        """High confidence match should be auto-approved."""
        # Identical aliases = high confidence
        result = score_candidate(
            sponsor_aliases=["pfizer", "pfe", "pfizer inc"],
            ticker_aliases=["pfizer", "pfe", "pfizer inc"],
        )
        assert result["confidence"] >= 0.9
        assert result["status"] == "approved"
        assert result["approved_by"] == "machine"
        assert result["rejected_by"] is None

    def test_score_candidate_low_confidence_rejected(self):
        """Low confidence match should be auto-rejected."""
        # No overlap = low confidence
        result = score_candidate(
            sponsor_aliases=["acme corp", "acme"],
            ticker_aliases=["globex", "gx"],
        )
        assert result["confidence"] < 0.5
        assert result["status"] == "rejected"
        assert result["approved_by"] is None
        assert result["rejected_by"] == "machine"

    def test_score_candidate_medium_confidence_pending(self):
        """Medium confidence match should be pending review."""
        # Partial overlap = medium confidence
        result = score_candidate(
            sponsor_aliases=["pharma corp", "pharma", "pc"],
            ticker_aliases=["pharma inc", "pharma", "pi"],
        )
        assert 0.5 <= result["confidence"] < 0.9
        assert result["status"] == "pending"
        assert result["approved_by"] is None
        assert result["rejected_by"] is None

    def test_score_candidates_mixed_statuses_schema(self):
        """score_candidates handles mixed statuses without schema errors.

        This is the critical test - Polars schema inference failed when
        early rows had None for approved_by/rejected_by and later rows
        had 'machine' strings. The DataFrame creation must use explicit schema.
        """
        # Create test DataFrame with candidates that will produce all status types
        candidates_df = pl.DataFrame(
            {
                "sponsor_name": ["A", "B", "C", "D"],
                "ticker": ["T1", "T2", "T3", "T4"],
                # High overlap → approved
                "sponsor_aliases": [
                    ["exact match", "em"],
                    ["partial", "match"],
                    ["no", "overlap"],
                    ["another", "exact", "test"],
                ],
                "ticker_aliases": [
                    ["exact match", "em"],  # Will be approved (high confidence)
                    ["partial", "different"],  # Will be pending (medium confidence)
                    ["completely", "different"],  # Will be rejected (low confidence)
                    ["another", "exact", "test"],  # Will be approved
                ],
            }
        )

        # This should NOT raise a schema error
        result = score_candidates(candidates_df)

        # Verify schema
        assert "confidence" in result.columns
        assert "status" in result.columns
        assert "approved_by" in result.columns
        assert "rejected_by" in result.columns

        # Verify we got mixed statuses
        statuses = result["status"].to_list()
        assert "approved" in statuses or "rejected" in statuses  # At least one decision
        assert len(result) == 4

    def test_score_candidates_empty_input(self):
        """Handle empty candidate DataFrame gracefully."""
        empty_df = pl.DataFrame(
            {
                "sponsor_name": [],
                "ticker": [],
                "sponsor_aliases": [],
                "ticker_aliases": [],
            }
        )

        result = score_candidates(empty_df)
        assert len(result) == 0

    def test_score_candidates_all_rejected_schema(self):
        """Schema works when all rows are rejected (all have rejected_by='machine')."""
        # All low-confidence matches
        candidates_df = pl.DataFrame(
            {
                "sponsor_name": ["A", "B", "C"],
                "ticker": ["T1", "T2", "T3"],
                "sponsor_aliases": [["x"], ["y"], ["z"]],
                "ticker_aliases": [["a"], ["b"], ["c"]],
            }
        )

        result = score_candidates(candidates_df)
        assert all(s == "rejected" for s in result["status"].to_list())
        assert all(r == "machine" for r in result["rejected_by"].to_list())


class TestGenerateAliases:
    """Integration tests for alias generation (require Ollama running)."""

    @pytest.fixture
    def ollama_available(self):
        """Check if Ollama is available for integration tests."""
        from src.pipelines.entity_match.aliases import check_ollama_available

        if not check_ollama_available():
            pytest.skip("Ollama not available - start with: ollama serve")

    def test_generate_aliases_returns_list(self, ollama_available):
        """Alias generation returns a list of strings."""
        from src.pipelines.entity_match.aliases import generate_aliases

        # Use a well-known company with description for consistent results
        result = generate_aliases(
            "Pfizer Inc.",
            "Pfizer Inc. discovers, develops, manufactures, and sells biopharmaceutical products worldwide.",
            backend="ollama",
        )

        assert isinstance(result, list)
        # LLM output is non-deterministic, but should usually return something
        assert all(isinstance(a, str) for a in result)

    def test_generate_aliases_known_company(self, ollama_available):
        """Known company gets relevant aliases."""
        from src.pipelines.entity_match.aliases import generate_aliases

        result = generate_aliases(
            "Johnson & Johnson",
            "Johnson & Johnson researches, develops, manufactures healthcare products.",
            backend="ollama",
        )

        # Should get at least the ticker or common abbreviation
        aliases_lower = [a.lower() for a in result]
        has_relevant = any(term in " ".join(aliases_lower) for term in ["jnj", "j&j", "johnson"])
        assert has_relevant, f"Expected relevant alias, got: {result}"
