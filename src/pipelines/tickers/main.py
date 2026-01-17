"""
Main entry point for ticker list pipeline.

Extracts ticker reference data from Massive API and loads to S3 Tables.
"""

import os

from .extract import fetch_tickers
from .load import load_tickers


def run(
    api_key: str | None = None,
    table_bucket_arn: str | None = None,
    region: str = "us-east-1",
    limit: int = 1000,
) -> None:
    """
    Run the ticker pipeline.
    
    Args:
        api_key: Massive API key (or set MASSIVE_API_KEY env var)
        table_bucket_arn: S3 Table Bucket ARN (or set TABLE_BUCKET_ARN env var)
        region: AWS region
        limit: Results per page when fetching
    """
    api_key = api_key or os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        raise ValueError("MASSIVE_API_KEY required")
    
    table_bucket_arn = table_bucket_arn or os.environ.get("TABLE_BUCKET_ARN")
    if not table_bucket_arn:
        raise ValueError("TABLE_BUCKET_ARN required")
    
    print("Fetching tickers from Massive API...")
    tickers = list(fetch_tickers(api_key, limit=limit))
    print(f"Fetched {len(tickers)} tickers")
    
    print(f"Loading to S3 Tables ({table_bucket_arn})...")
    load_tickers(
        tickers,
        table_bucket_arn=table_bucket_arn,
        region=region,
    )
    print("Done!")


if __name__ == "__main__":
    run()
