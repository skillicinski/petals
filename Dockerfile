# Dockerfile for petals pipelines
# Multi-stage build for smaller final image
# Platform fixed to amd64 for AWS Fargate compatibility

FROM --platform=linux/amd64 python:3.12-slim AS builder

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first (better layer caching)
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev --no-editable

# Copy source code
COPY src/ ./src/

# =================================================================
# Final stage - minimal runtime image
# =================================================================
FROM --platform=linux/amd64 python:3.12-slim

WORKDIR /app

# Copy virtual environment and source from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src

# Use the virtual environment
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Pipeline module to run - MUST be set via environment variable
# Examples:
#   src.pipelines.tickers.main
#   src.pipelines.trials.main
#   src.pipelines.ticker_details.main
ENTRYPOINT ["sh", "-c", "python -u -m ${PIPELINE:?PIPELINE env var must be set}"]
