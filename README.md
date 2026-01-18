# petals

A personal project demonstrating end-to-end data platform design and implementation.

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

# Run pipeline
export MASSIVE_API_KEY=$(grep MASSIVE_API_KEY .env | cut -d'=' -f2)
export AWS_PROFILE=personal
export TABLE_BUCKET_ARN="arn:aws:s3tables:us-east-1:<account>:bucket/petals-tables-<account>"
uv run python -m src.pipelines.tickers.main

# Run tests
uv run pytest tests/ -v
```

## Scope

- **Extract** - Ingest data from external sources
- **Transform** - Enrich and normalize raw data
- **Load** - Land processed data in S3 with a data catalog
- **Orchestration** - Schedule and monitor processes
- **Analytics** - Monitor platform performance and health