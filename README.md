# petals

A personal project demonstrating end-to-end data platform design and implementation.

## Scope

- **Extract** - Ingest data from external sources (Massive API, ClinicalTrials.gov)
- **Transform** - Enrich and normalize raw data
- **Load** - Land processed data in S3 Tables (Iceberg)
- **Orchestration** - Schedule and monitor processes via Step Functions

## Pipelines

| Pipeline | Description | Schedule |
|----------|-------------|----------|
| `tickers` | Stock/ETF ticker reference data from Massive API | Daily 6 AM UTC |
| `ticker_details` | Enriched ticker details (SIC codes, descriptions) for US stocks | Daily 7 AM UTC |
| `trials` | Completed clinical trials from ClinicalTrials.gov (INDUSTRY sponsors) | Daily 7 AM UTC |
| `entity_match` | LLM-powered sponsor↔ticker matching | On-demand |

## Setup

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh  # or: brew install uv

# Install dependencies
uv sync --all-extras

# Create .env with AWS config
cat > .env << 'EOF'
MASSIVE_API_KEY=your_key_here
AWS_ACCOUNT_ID=123456789012
AWS_PROFILE=personal
AWS_REGION=us-east-1
EOF
```

## Local Development

```bash
# Run pipelines locally
uv run python -m src.pipelines.tickers.main
uv run python -m src.pipelines.ticker_details.main
uv run python -m src.pipelines.trials.main

# Entity match requires Ollama for local LLM
ollama serve && ollama pull llama3.2:3b
LLM_BACKEND=ollama LIMIT_SPONSORS=50 LIMIT_TICKERS=150 \
  uv run python -m src.pipelines.entity_match.main

# Run tests
just test
```

## Deploy & Operations

Requires: AWS CLI configured, Secrets Manager secret `petals/Massive` with API key.

```bash
# Full deploy (sync → format → test → docker build/push → cdk deploy)
just deploy

# Skip tests if needed
just deploy skip_tests=true

# Check pipeline health
just health

# View logs / trigger pipeline
just logs tickers
just trigger tickers false
```

See `docs/` for architecture and design documentation.
