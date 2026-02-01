# Dockerfile for petals pipelines
# Uses pre-baked base image with ALL dependencies for fast iteration
#
# Base image rebuild (when pyproject.toml/uv.lock changes):
#   just build-base
#
# This Dockerfile only copies source code on top of the base image

FROM 620117234001.dkr.ecr.us-east-1.amazonaws.com/petals-base:latest

# Just copy source code - venv already in base image
COPY src/ ./src/

# Venv and env already set in base image
ENTRYPOINT ["sh", "-c", "python -u -m ${PIPELINE:?PIPELINE env var must be set}"]
