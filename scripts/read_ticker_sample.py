#!/usr/bin/env python3
"""Simple script to read ticker price data using Polars."""

import polars as pl
from datetime import datetime

# Configure Polars to show all columns
pl.Config.set_tbl_cols(9)

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
    'vol': pl.Int64,
    'openint': pl.Int64,
}

# Read the first 10 rows
df = pl.read_csv(
    'data/spooq_downloads/data_us/daily/us/nasdaq stocks/1/aapl.us.txt',
    separator=',',
    schema=schema,
    skip_rows=1,  # Skip the header row
    n_rows=1000,
)

# Drop unnecessary columns
df = df.drop('per')
df = df.drop('time')
df = df.drop('openint')

# Convert the date column to a date type then back to string to match table scheam
df = df.with_columns(pl.col('date').str.to_date(format='%Y%m%d'))
df = df.with_columns(pl.col('date').dt.to_string(format='%Y-%m-%d'))

# Add a last_fetched_utc column with the current datetime as a string
df = df.with_columns(last_fetched_utc=datetime.now())
df = df.with_columns(pl.col('last_fetched_utc').dt.to_string(format='%Y-%m-%d %H:%M:%S'))

# Convert volume to long/int64
df = df.with_columns(pl.col('vol').cast(pl.Int64))

# Split ticker into ticker and market
df = df.with_columns(
    pl.col('ticker')
    .str.split_exact('.', 1)
    .struct.rename_fields(['ticker', 'market'])
    .alias('ticker_market')
).drop('ticker').unnest('ticker_market')

df = df.select([
    'date',
    'ticker',
    'market',
    'locale'
    'open',
    'high',
    'low',
    'close',
    'volume',
    'last_fetched_utc',
])

print('Columns:', df.columns)
print(df)
