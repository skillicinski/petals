"""Load generated aliases to storage."""

import json
import os
from pathlib import Path

import polars as pl
import pyarrow as pa
from pyiceberg.catalog import load_catalog


def get_catalog():
    """Load PyIceberg catalog for S3 Tables."""
    bucket_arn = os.environ.get(
        'TABLE_BUCKET_ARN',
        'arn:aws:s3tables:us-east-1:620117234001:bucket/petals-tables-620117234001'
    )
    region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    return load_catalog(
        's3tables',
        type='rest',
        uri=f'https://s3tables.{region}.amazonaws.com/iceberg',
        warehouse=bucket_arn,
        **{
            'rest.sigv4-enabled': 'true',
            'rest.signing-region': region,
            'rest.signing-name': 's3tables',
        }
    )


def save_aliases_json(aliases: dict[str, list[str]], output_path: str | Path) -> None:
    """Save aliases to local JSON file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(aliases, f, indent=2)

    print(f'[load] Saved {len(aliases)} alias records to {output_path}')


def save_aliases_iceberg(df: pl.DataFrame) -> None:
    """Save aliases to Iceberg table.

    Creates table if it doesn't exist.
    Schema: ticker, market, name, aliases (list<string>)
    """
    catalog = get_catalog()

    # Check if table exists, create if not
    try:
        table = catalog.load_table('reference.ticker_aliases')
    except Exception:
        print('[load] Creating ticker_aliases table...')
        schema = pa.schema([
            ('ticker', pa.string()),
            ('market', pa.string()),
            ('name', pa.string()),
            ('aliases', pa.list_(pa.string())),
        ])
        table = catalog.create_table(
            'reference.ticker_aliases',
            schema=schema,
            properties={'format-version': '2'}
        )

    # Convert to Arrow and upsert
    arrow_table = df.to_arrow()
    table.upsert(arrow_table, join_cols=['ticker', 'market'])

    print(f'[load] Upserted {len(df)} alias records to ticker_aliases')


def build_aliases_dataframe(
    companies_df: pl.DataFrame,
    aliases_dict: dict[str, list[str]]
) -> pl.DataFrame:
    """Build DataFrame with aliases for Iceberg storage."""
    rows = []
    for row in companies_df.iter_rows(named=True):
        key = f"{row['ticker']}|{row['market']}"
        aliases = aliases_dict.get(key, [])
        rows.append({
            'ticker': row['ticker'],
            'market': row['market'],
            'name': row['name'],
            'aliases': aliases,
        })

    return pl.DataFrame(rows)
