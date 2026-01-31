"""Generate entity aliases using LLM.

Supports two backends:
- ollama: Local LLM via Ollama (requires `ollama serve` running)
- bedrock: AWS Bedrock (requires Bedrock model access enabled)

For local development with Ollama:
    1. Install: brew install ollama
    2. Start server: ollama serve
    3. Pull model: ollama pull llama3.2:3b
    4. Run pipeline: LLM_BACKEND=ollama python -m src.pipelines.entity_match.main
"""

import json
import os
import re
import sys
import time
from typing import Literal

import polars as pl

LLMBackend = Literal['ollama', 'bedrock']


class OllamaNotAvailableError(Exception):
    """Raised when Ollama server is not running."""
    pass


def check_ollama_available() -> bool:
    """Check if Ollama server is running and has required model."""
    try:
        import ollama
        models = ollama.list()
        model_names = [m.model for m in models.models]
        required = os.environ.get('OLLAMA_MODEL', 'llama3.2:3b')
        # Check for exact match or partial (llama3.2:3b matches llama3.2:3b-...)
        return any(required in name for name in model_names)
    except Exception:
        return False


def parse_aliases_response(text: str) -> list[str]:
    """Parse LLM response into list of alias strings."""
    text = text.strip()

    # Extract JSON array if embedded in other text
    if '[' not in text:
        return []

    try:
        start = text.index('[')
        end = text.rindex(']') + 1
        text = text[start:end]
    except ValueError:
        # No closing bracket found
        return []

    # Fix common JSON errors from small models
    # {"GSK"} -> "GSK" (object with no value)
    text = re.sub(r'\{"([^"]+)"\}', r'"\1"', text)
    text = re.sub(r"\{'([^']+)'\}", r'"\1"', text)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        # Try with single quotes converted to double
        text = text.replace("'", '"')
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []

    # Flatten if model returned objects instead of strings
    aliases = []
    for item in parsed:
        if isinstance(item, str):
            aliases.append(item)
        elif isinstance(item, dict):
            aliases.extend(str(v) for v in item.values())

    return aliases


def truncate_description(description: str | None, max_chars: int = 200) -> str | None:
    """Truncate long descriptions to avoid bloated prompts."""
    if not description:
        return None
    if len(description) <= max_chars:
        return description
    return description[:max_chars].rsplit(' ', 1)[0] + '...'


def generate_aliases_ollama(
    company_name: str,
    description: str | None = None,
    model: str = 'llama3.2:3b',
    timeout: float = 30.0,
) -> list[str]:
    """Generate aliases using local Ollama.

    Args:
        company_name: Company name to generate aliases for
        description: Optional company description for context
        model: Ollama model name
        timeout: Request timeout in seconds

    Raises:
        OllamaNotAvailableError: If Ollama server is not running or model not found.
        TimeoutError: If request takes longer than timeout.
    """
    import ollama
    from ollama import ResponseError

    if not check_ollama_available():
        raise OllamaNotAvailableError(
            f"Ollama not available. Start with: ollama serve && ollama pull {model}"
        )

    # Truncate long descriptions
    description = truncate_description(description)

    prompt = f'''List aliases for "{company_name}" as a valid JSON array of strings only.
'''
    if description:
        prompt += f'Context: {description}\n'

    prompt += '''Return ONLY the JSON array, no explanations. Example: ["Alias1", "Alias2"]
'''

    try:
        response = ollama.generate(
            model=model,
            prompt=prompt,
            options={
                'temperature': 0.1,
                'num_predict': 128,  # Limit output tokens
            },
            keep_alive='5m',  # Keep model loaded between calls
        )
    except ResponseError as e:
        raise RuntimeError(f"Ollama error: {e}")

    return parse_aliases_response(response['response'])


def generate_aliases_bedrock(
    company_name: str,
    description: str | None = None,
    model_id: str = 'meta.llama3-8b-instruct-v1:0'
) -> list[str]:
    """Generate aliases using AWS Bedrock."""
    import boto3

    bedrock = boto3.client('bedrock-runtime')

    # Truncate long descriptions
    description = truncate_description(description)

    prompt = f'''List aliases for "{company_name}" as a valid JSON array of strings only.
'''
    if description:
        prompt += f'Context: {description}\n'

    prompt += '''Return ONLY the JSON array, no explanations. Example: ["Alias1", "Alias2"]
'''

    # Llama 3 format
    body = json.dumps({
        'prompt': f'<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\n{prompt}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n',
        'max_gen_len': 128,  # Limit output tokens
        'temperature': 0.1,
    })

    response = bedrock.invoke_model(
        modelId=model_id,
        body=body,
        contentType='application/json',
        accept='application/json'
    )

    result = json.loads(response['body'].read())
    return parse_aliases_response(result.get('generation', ''))


def generate_aliases(
    company_name: str,
    description: str | None = None,
    backend: LLMBackend = 'ollama'
) -> list[str]:
    """Generate aliases using configured backend.

    Backend selection:
    - 'ollama': Local LLM via Ollama (default for dev)
    - 'bedrock': AWS Bedrock (for cloud deployment)

    Set LLM_BACKEND env var to override.
    """
    backend = os.environ.get('LLM_BACKEND', backend)

    if backend == 'ollama':
        model = os.environ.get('OLLAMA_MODEL', 'llama3.2:3b')
        return generate_aliases_ollama(company_name, description, model)
    elif backend == 'bedrock':
        model = os.environ.get('BEDROCK_MODEL', 'meta.llama3-8b-instruct-v1:0')
        return generate_aliases_bedrock(company_name, description, model)
    else:
        raise ValueError(f'Unknown LLM backend: {backend}')


def normalize_alias(alias: str) -> str:
    """Normalize an alias for comparison.

    - lowercase
    - strip whitespace
    - normalize unicode
    """
    return alias.lower().strip()


def enrich_with_aliases(
    df: pl.DataFrame,
    name_col: str,
    description_col: str | None = None,
    backend: LLMBackend = 'ollama',
    progress_interval: int = 10,
) -> pl.DataFrame:
    """Enrich DataFrame with aliases generated via LLM.

    Args:
        df: DataFrame with entity names
        name_col: Column containing entity names to generate aliases for
        description_col: Optional column with descriptions for context
        backend: LLM backend ('ollama' or 'bedrock')
        progress_interval: Print progress every N rows

    Returns:
        DataFrame with added 'aliases' column (list of normalized strings)
    """
    start_time = time.time()
    aliases_list = []
    processed = 0
    errors = 0

    for row in df.iter_rows(named=True):
        name = row[name_col]
        description = row.get(description_col) if description_col else None

        try:
            raw_aliases = generate_aliases(name, description, backend=backend)
            # Normalize and include original name
            aliases = [normalize_alias(name)]
            aliases.extend(normalize_alias(a) for a in raw_aliases)
            # Dedupe while preserving order
            seen = set()
            unique_aliases = []
            for a in aliases:
                if a not in seen:
                    seen.add(a)
                    unique_aliases.append(a)
            aliases_list.append(unique_aliases)
            processed += 1

        except Exception as e:
            print(f'[aliases] Error generating aliases for "{name}": {e}', file=sys.stderr)
            # Fall back to just the normalized name
            aliases_list.append([normalize_alias(name)])
            errors += 1

        if (processed + errors) % progress_interval == 0:
            elapsed = time.time() - start_time
            rate = (processed + errors) / elapsed if elapsed > 0 else 0
            print(f'[aliases] Progress: {processed + errors}/{len(df)} ({rate:.1f}/s)')

    elapsed = time.time() - start_time
    print(f'[aliases] Generated aliases for {processed} entities ({errors} errors) in {elapsed:.1f}s')

    return df.with_columns(pl.Series('aliases', aliases_list))
