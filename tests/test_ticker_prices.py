"""Tests for ticker prices (OHLC) pipeline."""

from datetime import datetime, timedelta

import polars as pl
import pyarrow as pa
import pytest

from src.pipelines.ticker_prices.extract import (
    fetch_ticker_prices_batch,
    get_latest_trading_day,
)
from src.pipelines.ticker_prices.load import (
    TICKER_PRICES_ARROW_SCHEMA,
    TICKER_PRICES_SCHEMA,
    prices_to_arrow,
)


class TestGetLatestTradingDay:
    """Tests for get_latest_trading_day function."""

    def test_returns_yesterday(self):
        """Returns yesterday's date in YYYY-MM-DD format."""
        result = get_latest_trading_day()
        expected = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        assert result == expected

    def test_format_is_correct(self):
        """Date format is YYYY-MM-DD."""
        result = get_latest_trading_day()
        # Should be parseable as date
        datetime.strptime(result, "%Y-%m-%d")


class TestFetchTickerPricesBatch:
    """Tests for fetch_ticker_prices_batch function."""

    def test_fetch_single_ticker(self):
        """Can fetch data for a single ticker."""
        tickers = ["AAPL"]
        prices = list(fetch_ticker_prices_batch(tickers, batch_size=1))

        assert len(prices) >= 0  # May be 0 if market is closed or no data
        if prices:
            assert prices[0]["ticker"] == "AAPL"
            assert "date" in prices[0]
            assert "close" in prices[0]

    def test_fetch_multiple_tickers(self):
        """Can fetch data for multiple tickers."""
        tickers = ["AAPL", "MSFT"]
        prices = list(fetch_ticker_prices_batch(tickers, batch_size=2))

        # Should get data for both (or neither if market issue)
        if prices:
            tickers_returned = {p["ticker"] for p in prices}
            assert tickers_returned.issubset({"AAPL", "MSFT"})

    def test_handles_invalid_ticker(self):
        """Handles invalid ticker gracefully (no exception)."""
        tickers = ["INVALID_TICKER_XYZ123"]
        prices = list(fetch_ticker_prices_batch(tickers, batch_size=1))
        # Should return empty or no data, but not crash
        assert isinstance(prices, list)

    def test_mixed_valid_invalid_tickers(self):
        """Can handle mix of valid and invalid tickers."""
        tickers = ["AAPL", "INVALID_XYZ"]
        prices = list(fetch_ticker_prices_batch(tickers, batch_size=2))

        # Should get at least AAPL if data is available
        assert isinstance(prices, list)
        if prices:
            # Valid tickers should be present
            valid_tickers = [p["ticker"] for p in prices]
            assert "AAPL" in valid_tickers or len(valid_tickers) == 0

    def test_output_structure(self):
        """Output has expected structure."""
        tickers = ["AAPL"]
        prices = list(fetch_ticker_prices_batch(tickers, batch_size=1))

        if prices:
            price = prices[0]
            # Check all required fields exist
            assert "ticker" in price
            assert "date" in price
            assert "open" in price
            assert "high" in price
            assert "low" in price
            assert "close" in price
            assert "volume" in price
            assert "last_fetched_utc" in price
            assert "market" in price
            assert "locale" in price

            # Date should be ISO format
            assert isinstance(price["date"], str)
            datetime.strptime(price["date"], "%Y-%m-%d")

            # last_fetched_utc should be ISO format with Z
            assert isinstance(price["last_fetched_utc"], str)
            assert price["last_fetched_utc"].endswith("Z")

            # market and locale should be strings
            assert isinstance(price["market"], str)
            assert isinstance(price["locale"], str)
            assert price["last_fetched_utc"].endswith("Z")

    def test_specific_date(self):
        """Can fetch data for a specific date."""
        # Use a date we know has data (not too recent)
        tickers = ["AAPL"]
        date = "2024-12-31"  # Known trading day
        prices = list(fetch_ticker_prices_batch(tickers, date=date, batch_size=1))

        if prices:
            assert prices[0]["date"] == date


class TestPricesToArrow:
    """Tests for prices_to_arrow conversion."""

    def test_converts_valid_prices(self):
        """Converts list of price dicts to Arrow table."""
        prices = [
            {
                "ticker": "AAPL",
                "date": "2024-01-01",
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 103.0,
                "volume": 1000000,
                "last_fetched_utc": "2024-01-02T00:00:00Z",
                "market": "stocks",
                "locale": "us",
            },
            {
                "ticker": "MSFT",
                "date": "2024-01-01",
                "open": 200.0,
                "high": 205.0,
                "low": 199.0,
                "close": 203.0,
                "volume": 2000000,
                "last_fetched_utc": "2024-01-02T00:00:00Z",
                "market": "stocks",
                "locale": "us",
            },
        ]

        table = prices_to_arrow(prices)

        assert isinstance(table, pa.Table)
        assert len(table) == 2
        assert table.schema.equals(TICKER_PRICES_ARROW_SCHEMA)

    def test_deduplicates_by_ticker_date(self):
        """Deduplicates rows with same (ticker, date) key."""
        prices = [
            {
                "ticker": "AAPL",
                "date": "2024-01-01",
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 103.0,
                "volume": 1000000,
                "last_fetched_utc": "2024-01-02T00:00:00Z",
                "market": "stocks",
                "locale": "us",
            },
            {
                "ticker": "AAPL",
                "date": "2024-01-01",
                "open": 101.0,  # Different values
                "high": 106.0,
                "low": 100.0,
                "close": 104.0,
                "volume": 1100000,
                "last_fetched_utc": "2024-01-02T01:00:00Z",
                "market": "stocks",
                "locale": "us",
            },
        ]

        table = prices_to_arrow(prices)

        # Should only have 1 row (deduplicated)
        assert len(table) == 1

    def test_handles_empty_list(self):
        """Handles empty price list."""
        table = prices_to_arrow([])
        assert isinstance(table, pa.Table)
        assert len(table) == 0

    def test_handles_null_values(self):
        """Handles null values in price data."""
        prices = [
            {
                "ticker": "AAPL",
                "date": "2024-01-01",
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "volume": None,
                "last_fetched_utc": "2024-01-02T00:00:00Z",
                "market": "stocks",
                "locale": "us",
            }
        ]

        table = prices_to_arrow(prices)
        assert len(table) == 1


class TestIcebergSchema:
    """Tests for Iceberg schema definition."""

    def test_schema_has_composite_key(self):
        """Schema defines (ticker, date) as composite key."""
        assert TICKER_PRICES_SCHEMA.identifier_field_ids == [1, 2]

    def test_schema_field_count(self):
        """Schema has expected number of fields."""
        # ticker, date, open, high, low, close, volume, last_fetched_utc, market, locale
        assert len(TICKER_PRICES_SCHEMA.fields) == 10

    def test_required_fields(self):
        """Only ticker and date are required."""
        required_fields = [f.name for f in TICKER_PRICES_SCHEMA.fields if f.required]
        assert set(required_fields) == {"ticker", "date"}

    def test_arrow_schema_matches_iceberg(self):
        """Arrow schema field names match Iceberg schema."""
        iceberg_names = {f.name for f in TICKER_PRICES_SCHEMA.fields}
        arrow_names = set(TICKER_PRICES_ARROW_SCHEMA.names)
        assert iceberg_names == arrow_names


class TestSchemaEvolution:
    """Test schema evolution functionality."""

    def test_evolve_schema_adds_missing_column(self):
        """Schema evolution adds missing columns from target schema."""
        from unittest.mock import MagicMock
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType, DoubleType
        from src.pipelines.ticker_prices.load import evolve_schema

        # Create a mock table with old schema (missing 'market' field)
        old_schema = Schema(
            NestedField(1, "ticker", StringType(), required=True),
            NestedField(2, "date", StringType(), required=True),
            NestedField(3, "close", DoubleType(), required=False),
        )

        # Target schema with new field
        new_schema = Schema(
            NestedField(1, "ticker", StringType(), required=True),
            NestedField(2, "date", StringType(), required=True),
            NestedField(3, "close", DoubleType(), required=False),
            NestedField(4, "market", StringType(), required=False),
        )

        # Mock catalog and table
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.schema.return_value = old_schema
        mock_catalog.load_table.return_value = mock_table

        # Mock update_schema context manager
        mock_update = MagicMock()
        mock_table.update_schema.return_value.__enter__.return_value = mock_update
        mock_table.update_schema.return_value.__exit__.return_value = None

        # Run evolution
        evolve_schema(mock_catalog, "namespace.table", new_schema)

        # Verify add_column was called for the missing field
        mock_update.add_column.assert_called_once()
        call_args = mock_update.add_column.call_args
        assert call_args[0][0] == "market"  # field name
        assert isinstance(call_args[0][1], StringType)  # field type

    def test_evolve_schema_no_changes_needed(self):
        """Schema evolution does nothing when schemas match."""
        from unittest.mock import MagicMock
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType
        from src.pipelines.ticker_prices.load import evolve_schema

        # Same schema
        schema = Schema(
            NestedField(1, "ticker", StringType(), required=True),
            NestedField(2, "date", StringType(), required=True),
        )

        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.schema.return_value = schema
        mock_catalog.load_table.return_value = mock_table

        # Run evolution
        evolve_schema(mock_catalog, "namespace.table", schema)

        # Verify update_schema was never called
        mock_table.update_schema.assert_not_called()


class TestIntegrationWithRealData:
    """Integration tests with real yfinance API calls."""

    @pytest.mark.slow
    def test_end_to_end_fetch_and_convert(self):
        """End-to-end test: fetch from yfinance and convert to Arrow."""
        # Use well-known tickers that should have data
        tickers = ["AAPL", "MSFT"]
        prices = list(fetch_ticker_prices_batch(tickers, batch_size=2))

        # Should get some data (unless market is closed or API issue)
        if prices:
            # Convert to Arrow
            table = prices_to_arrow(prices)

            # Verify table structure
            assert isinstance(table, pa.Table)
            assert len(table) > 0
            assert table.schema.equals(TICKER_PRICES_ARROW_SCHEMA)

            # Verify data types
            tickers_col = table.column("ticker").to_pylist()
            assert all(isinstance(t, str) for t in tickers_col)

            dates_col = table.column("date").to_pylist()
            assert all(isinstance(d, str) for d in dates_col)

    @pytest.mark.slow
    def test_handles_weekend_or_holiday(self):
        """Pipeline handles weekends/holidays gracefully."""
        # This test may return no data if run on weekend/holiday
        tickers = ["AAPL"]
        prices = list(fetch_ticker_prices_batch(tickers, batch_size=1))

        # Should not crash, even if no data
        assert isinstance(prices, list)
