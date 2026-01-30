"""Alias generation pipeline - main entry point.

Generates company name aliases using LLM for entity matching.

Environment variables:
    TABLE_BUCKET_ARN: S3 Tables bucket ARN
    LLM_BACKEND: 'ollama' (local) or 'bedrock' (cloud)
    OLLAMA_MODEL: Model name for Ollama (default: llama3.2:3b)
    BEDROCK_MODEL: Model ID for Bedrock (default: meta.llama3-8b-instruct-v1:0)
    OUTPUT_PATH: Path for JSON output (default: data/aliases.json)
    LIMIT: Max companies to process (default: all)
    SAVE_ICEBERG: Set to '1' to save to Iceberg table

Usage:
    # Local with Ollama (start `ollama serve` first)
    LLM_BACKEND=ollama LIMIT=10 python -m src.pipelines.alias_gen.main

    # Cloud with Bedrock
    LLM_BACKEND=bedrock python -m src.pipelines.alias_gen.main
"""

import os
import sys
import time

from .extract import fetch_companies, fetch_existing_aliases
from .load import build_aliases_dataframe, save_aliases_iceberg, save_aliases_json
from .transform import generate_aliases


def run_pipeline():
    """Run the alias generation pipeline."""
    start_time = time.time()

    # Configuration
    limit = int(os.environ.get('LIMIT', 0)) or None
    output_path = os.environ.get('OUTPUT_PATH', 'data/aliases.json')
    save_iceberg = os.environ.get('SAVE_ICEBERG', '0') == '1'
    backend = os.environ.get('LLM_BACKEND', 'ollama')

    print(f'[main] Starting alias generation pipeline')
    print(f'[main] Backend: {backend}, Limit: {limit or "all"}')

    # Extract
    companies_df = fetch_companies(limit=limit)
    existing_aliases = fetch_existing_aliases()

    # Transform - generate aliases for each company
    aliases_dict: dict[str, list[str]] = {}
    processed = 0
    skipped = 0

    for i, row in enumerate(companies_df.iter_rows(named=True)):
        ticker = row['ticker']
        market = row['market']
        name = row['name']
        description = row['description']
        key = f'{ticker}|{market}'

        # Skip if already have aliases
        if key in existing_aliases and existing_aliases[key]:
            skipped += 1
            continue

        # Generate aliases
        try:
            aliases = generate_aliases(name, description, backend=backend)
            aliases_dict[key] = aliases
            processed += 1

            if processed % 10 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed
                print(f'[main] Processed {processed}/{len(companies_df)} ({rate:.1f}/s)')

        except Exception as e:
            print(f'[main] Error processing {ticker}: {e}', file=sys.stderr)
            aliases_dict[key] = []

    print(f'[main] Generated aliases for {processed} companies, skipped {skipped}')

    # Load - save results
    save_aliases_json(aliases_dict, output_path)

    if save_iceberg:
        result_df = build_aliases_dataframe(companies_df, aliases_dict)
        save_aliases_iceberg(result_df)

    elapsed = time.time() - start_time
    print(f'[main] Pipeline complete in {elapsed:.1f}s')

    return aliases_dict


if __name__ == '__main__':
    run_pipeline()
