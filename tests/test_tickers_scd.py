"""
Unit tests for ticker pipeline data transformation.
"""

import pyarrow as pa

from src.pipelines.tickers.load import TICKER_ARROW_SCHEMA, tickers_to_arrow


class TestTickersToArrow:
    """Tests for tickers_to_arrow conversion."""

    def test_converts_ticker_dicts_to_arrow(self):
        """Basic conversion of ticker dicts to Arrow table."""
        tickers = [
            {
                'ticker': 'AAPL',
                'name': 'Apple Inc.',
                'market': 'stocks',
                'locale': 'us',
                'primary_exchange': 'XNAS',
                'type': 'CS',
                'active': True,
                'currency_name': 'usd',
                'cik': '0000320193',
                'composite_figi': 'BBG000B9XRY4',
                'delisted_utc': None,
                'last_updated_utc': '2024-01-15T00:00:00Z',
            },
        ]

        result = tickers_to_arrow(tickers)

        assert isinstance(result, pa.Table)
        assert len(result) == 1
        assert result.schema == TICKER_ARROW_SCHEMA
        assert result['ticker'][0].as_py() == 'AAPL'
        assert result['active'][0].as_py() is True

    def test_skips_missing_required_fields(self):
        """Records missing ticker or market are skipped."""
        tickers = [
            {'ticker': 'TEST'},  # Missing market - skipped
            {'market': 'stocks'},  # Missing ticker - skipped
            {'ticker': 'VALID', 'market': 'stocks'},  # Valid
        ]

        result = tickers_to_arrow(tickers)

        assert len(result) == 1
        assert result['ticker'][0].as_py() == 'VALID'

    def test_handles_delisted_ticker(self):
        """Conversion includes delisted_utc for delisted tickers."""
        tickers = [
            {
                'ticker': 'TWTR',
                'market': 'stocks',
                'name': 'Twitter, Inc.',
                'active': False,
                'delisted_utc': '2022-10-27',
            },
        ]

        result = tickers_to_arrow(tickers)

        assert result['ticker'][0].as_py() == 'TWTR'
        assert result['active'][0].as_py() is False
        assert result['delisted_utc'][0].as_py() == '2022-10-27'

    def test_handles_empty_list(self):
        """Conversion handles empty ticker list."""
        result = tickers_to_arrow([])

        assert len(result) == 0
        assert result.schema == TICKER_ARROW_SCHEMA

    def test_handles_multiple_tickers(self):
        """Conversion handles multiple tickers."""
        tickers = [
            {'ticker': 'AAPL', 'market': 'stocks', 'name': 'Apple Inc.', 'active': True},
            {'ticker': 'GOOG', 'market': 'stocks', 'name': 'Alphabet Inc.', 'active': True},
            {'ticker': 'MSFT', 'market': 'stocks', 'name': 'Microsoft Corporation', 'active': True},
        ]

        result = tickers_to_arrow(tickers)

        assert len(result) == 3
        tickers_list = result['ticker'].to_pylist()
        assert 'AAPL' in tickers_list
        assert 'GOOG' in tickers_list
        assert 'MSFT' in tickers_list

    def test_deduplicates_by_ticker_market(self):
        """Duplicate (ticker, market) pairs are deduplicated (last occurrence wins)."""
        tickers = [
            {'ticker': 'AAPL', 'market': 'stocks', 'name': 'Apple Old Name', 'active': True},
            {'ticker': 'GOOG', 'market': 'stocks', 'name': 'Alphabet Inc.', 'active': True},
            {'ticker': 'AAPL', 'market': 'stocks', 'name': 'Apple Inc.', 'active': True},  # Duplicate
        ]

        result = tickers_to_arrow(tickers)

        assert len(result) == 2  # Deduplicated
        aapl = [r for r in result.to_pylist() if r['ticker'] == 'AAPL'][0]
        assert aapl['name'] == 'Apple Inc.'  # Last occurrence wins

    def test_same_ticker_different_markets_kept(self):
        """Same ticker in different markets are kept as separate rows."""
        tickers = [
            {'ticker': 'BITW', 'market': 'otc', 'name': 'BITWISE OTC', 'active': True},
            {'ticker': 'BITW', 'market': 'stocks', 'name': 'Bitwise ETF', 'active': True},
        ]

        result = tickers_to_arrow(tickers)

        assert len(result) == 2  # Both kept - different markets
