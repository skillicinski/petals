"""Tests for entity matching pipeline."""

import pytest

from src.pipelines.entity_match.aliases import parse_aliases_response


class TestParseAliasesResponse:
    """Tests for LLM response parsing."""

    def test_parse_clean_json_array(self):
        """Parse a clean JSON array of strings."""
        response = '["GSK", "GlaxoSmithKline", "Glaxo"]'
        result = parse_aliases_response(response)
        assert result == ['GSK', 'GlaxoSmithKline', 'Glaxo']

    def test_parse_with_surrounding_text(self):
        """Extract JSON array embedded in other text."""
        response = 'Here are the aliases: ["GSK", "Glaxo"] Hope this helps!'
        result = parse_aliases_response(response)
        assert result == ['GSK', 'Glaxo']

    def test_parse_single_quotes(self):
        """Handle single-quoted JSON (common LLM error)."""
        response = "['GSK', 'Glaxo']"
        result = parse_aliases_response(response)
        assert result == ['GSK', 'Glaxo']

    def test_parse_invalid_object_syntax(self):
        """Handle {"GSK"} instead of "GSK" (common LLM error)."""
        response = '[{"GSK"}, "Glaxo", {"SmithKline"}]'
        result = parse_aliases_response(response)
        assert 'GSK' in result
        assert 'Glaxo' in result
        assert 'SmithKline' in result

    def test_parse_nested_objects(self):
        """Flatten nested objects like {'alias': 'GSK'}."""
        response = '[{"alias": "GSK"}, {"ticker": "NVS"}, "Novartis"]'
        result = parse_aliases_response(response)
        assert 'GSK' in result
        assert 'NVS' in result
        assert 'Novartis' in result

    def test_parse_empty_response(self):
        """Return empty list for empty response."""
        assert parse_aliases_response('') == []
        assert parse_aliases_response('No aliases found.') == []

    def test_parse_empty_array(self):
        """Parse empty JSON array."""
        assert parse_aliases_response('[]') == []

    def test_parse_invalid_json(self):
        """Return empty list for unparseable JSON."""
        response = '[GSK, Glaxo'  # Missing quotes and bracket
        result = parse_aliases_response(response)
        assert result == []


class TestGenerateAliases:
    """Integration tests for alias generation (require Ollama running)."""

    @pytest.fixture
    def ollama_available(self):
        """Check if Ollama is available for integration tests."""
        from src.pipelines.entity_match.aliases import check_ollama_available
        if not check_ollama_available():
            pytest.skip('Ollama not available - start with: ollama serve')

    def test_generate_aliases_returns_list(self, ollama_available):
        """Alias generation returns a list of strings."""
        from src.pipelines.entity_match.aliases import generate_aliases

        # Use a well-known company with description for consistent results
        result = generate_aliases(
            'Pfizer Inc.',
            'Pfizer Inc. discovers, develops, manufactures, and sells biopharmaceutical products worldwide.',
            backend='ollama'
        )

        assert isinstance(result, list)
        # LLM output is non-deterministic, but should usually return something
        assert all(isinstance(a, str) for a in result)

    def test_generate_aliases_known_company(self, ollama_available):
        """Known company gets relevant aliases."""
        from src.pipelines.entity_match.aliases import generate_aliases

        result = generate_aliases(
            'Johnson & Johnson',
            'Johnson & Johnson researches, develops, manufactures healthcare products.',
            backend='ollama'
        )

        # Should get at least the ticker or common abbreviation
        aliases_lower = [a.lower() for a in result]
        has_relevant = any(
            term in ' '.join(aliases_lower)
            for term in ['jnj', 'j&j', 'johnson']
        )
        assert has_relevant, f"Expected relevant alias, got: {result}"
