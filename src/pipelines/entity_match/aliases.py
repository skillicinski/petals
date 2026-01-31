"""Transform company names into aliases using LLM.

Supports two backends:
- ollama: Local LLM via Ollama (requires `ollama serve` running)
- bedrock: AWS Bedrock (requires Bedrock model access enabled)

For local development with Ollama:
    1. Install: brew install ollama
    2. Start server: ollama serve
    3. Pull model: ollama pull llama3.2:3b
    4. Run pipeline: LLM_BACKEND=ollama python -m src.pipelines.alias_gen.main
"""

import json
import os
import re
from typing import Literal

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


def generate_aliases_ollama(
    company_name: str,
    description: str | None = None,
    model: str = 'llama3.2:3b'
) -> list[str]:
    """Generate aliases using local Ollama.

    Raises:
        OllamaNotAvailableError: If Ollama server is not running or model not found.
    """
    import ollama

    if not check_ollama_available():
        raise OllamaNotAvailableError(
            f"Ollama not available. Start with: ollama serve && ollama pull {model}"
        )

    prompt = f'''List known aliases for this company as a JSON array of strings.

Company: {company_name}
'''
    if description:
        prompt += f'Description: {description}\n'

    prompt += '''
Include: ticker symbols, abbreviations, former names, common short names.
Format: ["alias1", "alias2", "alias3"]
Response:'''

    response = ollama.generate(
        model=model,
        prompt=prompt,
        options={'temperature': 0.1}
    )

    return parse_aliases_response(response['response'])


def generate_aliases_bedrock(
    company_name: str,
    description: str | None = None,
    model_id: str = 'meta.llama3-8b-instruct-v1:0'
) -> list[str]:
    """Generate aliases using AWS Bedrock."""
    import boto3

    bedrock = boto3.client('bedrock-runtime')

    prompt = f'''List known aliases for this company as a JSON array of strings.

Company: {company_name}
'''
    if description:
        prompt += f'Description: {description}\n'

    prompt += '''
Include: ticker symbols, abbreviations, former names, common short names.
Format: ["alias1", "alias2", "alias3"]
Response:'''

    # Llama 3 format
    body = json.dumps({
        'prompt': f'<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\n{prompt}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n',
        'max_gen_len': 256,
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
