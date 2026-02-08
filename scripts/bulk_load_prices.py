#!/usr/bin/env python3
"""
A script for bulk loading .txt files containing daily historical ticker OHLC data.

Looks for files in the given directory and subdirectories recursively.

Appends instead of upserting for better speed, which means it has a high chance of introducing duplicates.
Validate the composite key (ticker, date) to ensure uniqueness in source data before appending.


"""

import sys
import os
import argparse
import polars as pl
import glob
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from pipelines.ticker_prices.load import (
    get_catalog,
    ensure_namespace,
    TICKER_PRICES_SCHEMA,
    TICKER_PRICES_PARTITION_SPEC,
    prices_to_arrow,
)
from pyiceberg.exceptions import CommitFailedException

def log(msg: str):
    print(msg)
    sys.stdout.flush()

def main():
    
    parser = argparse.ArgumentParser(description='Bulk load ticker prices from directory')
    parser.add_argument(
        'directory',
        help='Directory to load (e.g., "data/spooq_downloads/data_us/daily/us/nasdaq etfs")'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be loaded without writing to table'
    )
    args = parser.parse_args()
    
    directory = args.directory.rstrip('/')
    
    if not os.path.exists(directory):
        log(f'Error: Directory not found: {directory}')
        sys.exit(1)
    
    # Extract exchange and market from path
    # Example: data/spooq_downloads/data_us/daily/us/nasdaq stocks
    path_parts = directory.split('/')
    exchange_market = path_parts[-1]  # e.g., "nasdaq stocks"
    parts = exchange_market.split(' ')
    exchange = parts[0] if len(parts) > 0 else 'unknown'
    market = parts[1] if len(parts) > 1 else 'unknown'
    
    log(f'Processing directory: {directory}')
    log(f'Exchange: {exchange}, Market: {market}')
    
    pl.Config.set_tbl_cols(10)

    schema = {
        'ticker': pl.Utf8,
        'per': pl.Utf8,
        'date': pl.Utf8,
        'time': pl.Utf8,
        'open': pl.Float64,
        'high': pl.Float64,
        'low': pl.Float64,
        'close': pl.Float64,
        'vol': pl.Float64,
        'openint': pl.Int64,
    }

    # Recursively find all .txt files
    files = get_files_recursive(directory)
    
    log(f'Found {len(files)} files to process')
    
    if args.dry_run:
        log('\n[DRY RUN] Showing first 10 files:')
        for f in files[:10]:
            log(f'  {f}')
        if len(files) > 10:
            log(f'  ... and {len(files) - 10} more')
        log(f'\n[DRY RUN] Would load {len(files)} files with market="{market}"')
        log('[DRY RUN] Run without --dry-run to proceed')
        return
    
    results = []

    for i, file in enumerate(files, 1):
        if i % 100 == 0:
            log(f'Processing file {i}/{len(files)} ({i/len(files)*100:.1f}%)...')
        df = load_file(file, schema, market)
        if df.is_empty():
            continue
        results.extend(df.to_dicts())

    log(f'Loaded {len(results):,} total records from {len(files)} files')
    
    # Validate composite key
    validate_composite_key(results)
    
    # Get table bucket ARN
    table_bucket_arn = os.environ.get(
        'TABLE_BUCKET_ARN',
        'arn:aws:s3tables:us-east-1:620117234001:bucket/petals-tables-620117234001'
    )
    
    log(f'\n[load] Using partition spec: {TICKER_PRICES_PARTITION_SPEC}')
    log('[load] Data will be partitioned by month(date) for query optimization\n')
    
    bulk_append(results, table_bucket_arn)

def get_files_recursive(directory: str):
    """Recursively find all .txt files in directory and subdirectories."""
    pattern = os.path.join(directory, '**', '*.txt')
    return sorted(glob.glob(pattern, recursive=True))

def validate_composite_key(prices: list[dict]):
    log('\n[validation] Checking (ticker, date) composite key uniqueness...')
    key_counts = {}
    for p in prices:
        key = (p.get('ticker'), p.get('date'))
        key_counts[key] = key_counts.get(key, 0) + 1
    
    duplicates = {k: v for k, v in key_counts.items() if v > 1}
    
    log(f'[validation] Total records: {len(prices):,}')
    log(f'[validation] Unique (ticker, date) pairs: {len(key_counts):,}')
    
    if duplicates:
        log(f'[validation] Warning, duplicates found: {len(duplicates):,} duplicate keys')
        log(f'[validation] Sample duplicates (first 10):')
        for (ticker, date), count in list(duplicates.items())[:10]:
            log(f'  {ticker}, {date}: {count} occurrences')
        raise ValueError(f'Composite key not unique! Found {len(duplicates)} duplicates.')
    else:
        log(f'[validation] Composite key (ticker, date) is unique')

def load_file(file: str, schema: dict, market: str):
    try:
        df = pl.read_csv(
            source=file,
            separator=',',
            schema=schema,
            skip_rows=1,
            n_rows=None,
        )
        
        df = df.drop('per', 'time', 'openint')
        df = df.with_columns(pl.lit(market).alias('market'))
        
        df = df.with_columns(pl.col('date').str.to_date(format='%Y%m%d'))
        df = df.with_columns(pl.col('date').dt.to_string(format='%Y-%m-%d'))
        
        df = df.with_columns(last_fetched_utc=datetime.now())
        df = df.with_columns(pl.col('last_fetched_utc').dt.to_string(format='%Y-%m-%d %H:%M:%S'))
        
        df = df.with_columns(pl.col('vol').cast(pl.Int64).alias('volume'))
        df = df.drop('vol')
        
        df = df.with_columns(
            pl.col('ticker')
            .str.split_exact('.', 1)
            .struct.rename_fields(['ticker', 'locale'])
            .alias('ticker_locale')
        ).drop('ticker').unnest('ticker_locale')
        
        df = df.with_columns(pl.col('locale').str.to_lowercase())
        
        df = df.select([
            'ticker', 'date', 'open', 'high', 'low', 'close',
            'volume', 'last_fetched_utc', 'locale', 'market'
        ])
        
        return df
    except Exception as e:
        log(f'Error reading {file}: {e}')
        return pl.DataFrame()

def bulk_append(
    prices: list[dict],
    table_bucket_arn: str,
    namespace: str = 'market_data',
    table_name: str = 'ticker_prices',
    region: str = 'us-east-1',
    batch_size: int = 50000,
):
    """
    Bulk append using larger batches for better performance.
    
    Use this for initial loads or backfills where you know data is new.
    For incremental updates with potential duplicates, use load_ticker_prices with upsert.
    """
    log(f'[bulk] Starting bulk append with {len(prices):,} records')
    log(f'[bulk] Batch size: {batch_size:,}')
    
    log('[bulk] Getting catalog...')
    catalog = get_catalog(table_bucket_arn, region)
    
    ensure_namespace(catalog, namespace)
    
    table_id = f'{namespace}.{table_name}'
    
    log('[bulk] Converting to Arrow table...')
    arrow_table = prices_to_arrow(prices)
    log(f'[bulk] Arrow table memory: {arrow_table.nbytes / 1024 / 1024:.2f} MB')
    
    from pyiceberg.exceptions import NoSuchTableError
    
    try:
        table = catalog.load_table(table_id)
        log(f'[bulk] Table exists: {table_id}')
        
        # Process in batches
        num_rows = len(arrow_table)
        num_batches = (num_rows + batch_size - 1) // batch_size
        
        log(f'[bulk] Processing {num_batches} batches...')
        
        for i in range(num_batches):
            start = i * batch_size
            end = min((i + 1) * batch_size, num_rows)
            batch = arrow_table.slice(start, end - start)
            
            log(f'[bulk] Batch {i + 1}/{num_batches}: appending rows {start:,}-{end:,}...')
            
            # Wait and refresh with backoff to handle CommitFailedException
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    table.append(batch)
                    break
                except CommitFailedException as e:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        log(f'[bulk] Commit failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s...')
                        time.sleep(wait_time)
                        table = catalog.load_table(table_id)
                    else:
                        log(f'[bulk] Failed to append batch number {i + 1} after {max_retries} attempts')
                        raise
            
            log(f'[bulk] Batch {i + 1} complete')
        
        log(f'[bulk] Bulk append complete: {num_rows:,} records appended')
        return {'rows_inserted': num_rows}
        
    except NoSuchTableError:
        log(f'[bulk] Creating table: {table_id}')
        table = catalog.create_table(
            table_id,
            schema=TICKER_PRICES_SCHEMA,
            partition_spec=TICKER_PRICES_PARTITION_SPEC
        )
        log('[bulk] Table created, appending all data...')
        table.append(arrow_table)
        log(f'[bulk] Initial load complete: {len(arrow_table):,} records')
        return {'rows_inserted': len(arrow_table)}

if __name__ == '__main__':
    main()
