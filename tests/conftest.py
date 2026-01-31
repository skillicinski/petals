"""
Pytest fixtures for LocalStack-based integration testing.

LocalStack emulates AWS services locally via Docker. These fixtures provide
boto3 clients that point to LocalStack instead of real AWS.

Usage:
    pytest tests/              # runs all tests
    pytest tests/ -k batch     # runs only batch-related tests

Requirements:
    - Docker must be running
    - testcontainers[localstack] must be installed (in dev dependencies)
"""

import os
import sys
from pathlib import Path

import re

import boto3
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

# Add project root to path so tests can import from src
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Pin LocalStack version for reproducible tests
LOCALSTACK_IMAGE = "localstack/localstack:4.4.0"


@pytest.fixture(scope="session")
def localstack():
    """
    Session-scoped LocalStack container.

    Starts once per test session and is shared across all tests.
    Services are started on-demand when first accessed.
    """
    container = (
        DockerContainer(image=LOCALSTACK_IMAGE)
        .with_exposed_ports(4566)
        .with_env("SERVICES", "s3,iam,batch,logs")
        .waiting_for(LogMessageWaitStrategy(re.compile(r"Ready\.\n")))
    )
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(4566)
        endpoint = f"http://{host}:{port}"

        # Set environment so boto3 clients can be configured
        os.environ["LOCALSTACK_ENDPOINT"] = endpoint
        os.environ["AWS_ACCESS_KEY_ID"] = "test"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        # Attach URL method for compatibility
        container.get_url = lambda: endpoint

        yield container


@pytest.fixture
def aws_endpoint(localstack) -> str:
    """Get the LocalStack endpoint URL."""
    return localstack.get_url()


@pytest.fixture
def s3_client(localstack):
    """Boto3 S3 client pointing to LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=localstack.get_url(),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


@pytest.fixture
def batch_client(localstack):
    """Boto3 Batch client pointing to LocalStack."""
    return boto3.client(
        "batch",
        endpoint_url=localstack.get_url(),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


@pytest.fixture
def iam_client(localstack):
    """Boto3 IAM client pointing to LocalStack."""
    return boto3.client(
        "iam",
        endpoint_url=localstack.get_url(),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


@pytest.fixture
def logs_client(localstack):
    """Boto3 CloudWatch Logs client pointing to LocalStack."""
    return boto3.client(
        "logs",
        endpoint_url=localstack.get_url(),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
