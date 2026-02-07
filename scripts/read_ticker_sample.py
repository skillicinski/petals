#!/usr/bin/env python3
"""Simple script to read ticker price data using Polars from a directory of files."""

import os
import polars as pl
import pyiceberg
from datetime import datetime

def main():
    # Configure Polars to show all columns
    pl.Config.set_tbl_cols(10)

    # Define the schema
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

    files = get_files('data/spooq_downloads/data_us/daily/us/nasdaq stocks/1')

    results = pl.DataFrame()

    for file in files:
        df = load_file(file, schema)
        print('Columns:', df.columns)
        print('Head:', df.head(10))
        if df.is_empty():
            print('Skipping empty dataframe')
            continue
        results = results.vstack(df)

    print('Results:', results.shape)
    return results

    write_file_to_table(results)

def get_files(directory: str):
    return [f"{directory}/{x}" for x in os.listdir(directory) if x.endswith('.txt')]

def load_file(file: str, schema: dict):
    # Read the first 10 rows
    try:
        df = pl.read_csv(
            source=file,
            separator=',',
            schema=schema,
            skip_rows=1,  # Skip the header row
            n_rows=1000,
        )

        print('Loading file:', file)
        # Example file path: 'data/spooq_downloads/data_us/daily/us/nasdaq stocks/1/aapl.us.txt'
        # Get the locale: 'us'
        locale = file.split('/')[2].split('_')[1]
        print('Locale:', locale)
        # Get the exchange: 'nasdaq'
        exchange = file.split('/')[5].split(' ')[0]
        print('Exchange:', exchange)
        # Get the market: 'stocks'
        market = file.split('/')[5].split(' ')[1]
        print('Market:', market)


        # Drop unnecessary columns
        df = df.drop('per')
        df = df.drop('time')
        df = df.drop('openint')

        # Add the locale, exchange, and market columns
        # df = df.with_columns(pl.lit(locale).alias('locale'))
        # df = df.with_columns(pl.lit(exchange).alias('exchange'))
        df = df.with_columns(pl.lit(market).alias('market'))

        # Convert the date column to a date type then back to string to match table scheam
        df = df.with_columns(pl.col('date').str.to_date(format='%Y%m%d'))
        df = df.with_columns(pl.col('date').dt.to_string(format='%Y-%m-%d'))

        # Add a last_fetched_utc column with the current datetime as a string
        df = df.with_columns(last_fetched_utc=datetime.now())
        df = df.with_columns(pl.col('last_fetched_utc').dt.to_string(format='%Y-%m-%d %H:%M:%S'))

        # Convert volume to float64 and change the column name to volume
        df = df.with_columns(pl.col('vol').cast(pl.Float64).alias('volume'))

        # Split ticker into ticker and market
        df = df.with_columns(
            pl.col('ticker')
            .str.split_exact('.', 1)
            .struct.rename_fields(['ticker', 'locale'])
            .alias('ticker_locale')
        ).drop('ticker').unnest('ticker_locale')

        # Convert locale to lowercase
        df = df.with_columns(pl.col('locale').str.to_lowercase())

        # Rearrange the columns
        df = df.select([
            'ticker',
            'date',
            'open',
            'high',
            'low',
            'close',
            'volume',
            'last_fetched_utc',
            'locale',
            'market',
        ])

        return df
    
    except Exception as e:
        print('Error reading file:', e)
        # Skip the file but continue with the next file
        return pl.DataFrame()

def write_file_to_table(df: pl.DataFrame, table_bucket_arn: str, table_id: str):
    # Write the dataframe as parquet to an S3 Table Bucket
    catalog = pyiceberg.get_catalog(table_bucket_arn)
    table = catalog.load_table(table_id)
    table.upsert(df)
    print('Table upserted')
    return True

if __name__ == '__main__':
    main()
