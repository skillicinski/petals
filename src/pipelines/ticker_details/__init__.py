"""Ticker details enrichment pipeline."""

from .extract import fetch_ticker_details as fetch_ticker_details
from .load import load_ticker_details as load_ticker_details
from .main import run as run
