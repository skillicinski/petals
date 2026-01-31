# petals

A personal project demonstrating end-to-end data platform design and implementation.

## Scope

- **Extract** - Ingest data from external sources (Massive API, ClinicalTrials.gov)
- **Transform** - Enrich and normalize raw data
- **Load** - Land processed data in S3 with a data catalog
- **Orchestration** - Schedule and monitor processes
- **Analytics** - Monitor platform performance and health

## Pipelines

| Pipeline | Description | Schedule |
|----------|-------------|----------|
| `tickers` | Stock/ETF ticker reference data from Massive API | Daily 6 AM UTC |
| `ticker_details` | Enriched ticker details (SIC codes, descriptions) for US stocks | Daily 7 AM UTC |
| `trials` | Completed clinical trials from ClinicalTrials.gov (INDUSTRY sponsors) | Daily 7 AM UTC |
| `entity_match` | LLM-powered sponsor↔ticker matching | Weekly Sunday 2 AM UTC |

## Setup

```bash
# Install uv using curl (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh
# Install uv using brew
brew install uv

# Install dependencies
uv sync --all-extras
```

## Local Development

```bash
# Create .env with your API key
echo "MASSIVE_API_KEY=your_key_here" > .env

# Configure AWS CLI profile (e.g., "personal")
aws configure --profile personal

# Common environment variables
export MASSIVE_API_KEY=$(grep MASSIVE_API_KEY .env | cut -d'=' -f2)
export AWS_PROFILE=personal
export TABLE_BUCKET_ARN="arn:aws:s3tables:us-east-1:<account>:bucket/petals-tables-<account>"

# Run pipelines
uv run python -m src.pipelines.tickers.main
uv run python -m src.pipelines.ticker_details.main
uv run python -m src.pipelines.trials.main

# Entity match requires Ollama for local LLM testing
ollama serve && ollama pull llama3.2:3b
LLM_BACKEND=ollama LIMIT_SPONSORS=50 LIMIT_TICKERS=150 \
  uv run python -m src.pipelines.entity_match.main

# Run tests
uv run pytest tests/ -v
cd cdk && uv run pytest tests/ -v
```

## Deploy to AWS

```bash
# Prerequisites: AWS CLI configured, Secrets Manager secret "petals/Massive" with API key

# Deploy infrastructure (all stacks)
cd cdk
uv run cdk deploy petals-shared \
  petals-tickers-pipeline \
  petals-ticker-details-pipeline \
  petals-trials-pipeline \
  petals-entity-match-pipeline

# Build and push container (single image for all pipelines)
docker build -t petals-pipelines .
aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com
docker tag petals-pipelines:latest <account>.dkr.ecr.us-east-1.amazonaws.com/petals-pipelines:latest
docker push <account>.dkr.ecr.us-east-1.amazonaws.com/petals-pipelines:latest
```

See `docs/` for architecture and design documentation.
