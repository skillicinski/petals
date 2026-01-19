# Dockerfile for petals pipelines
# Multi-stage build for smaller final image

FROM python:3.12-slim AS builder

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
FROM python:3.12-slim

WORKDIR /app

# Copy virtual environment and source from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src

# Use the virtual environment
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Default entrypoint runs tickers pipeline
# Override with different -m for other pipelines
ENTRYPOINT ["python", "-u", "-m", "src.pipelines.tickers.main"]
