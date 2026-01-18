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

import boto3
import pytest
from testcontainers.localstack import LocalStackContainer

# Pin LocalStack version for reproducible tests
# v4.8+ has native Batch support
LOCALSTACK_IMAGE = "localstack/localstack:4.4.0"


@pytest.fixture(scope="session")
def localstack():
    """
    Session-scoped LocalStack container.

    Starts once per test session and is shared across all tests.
    Services are started on-demand when first accessed.
    """
    with LocalStackContainer(image=LOCALSTACK_IMAGE) as container:
        # Set environment so boto3 clients can be configured
        os.environ["LOCALSTACK_ENDPOINT"] = container.get_url()
        os.environ["AWS_ACCESS_KEY_ID"] = "test"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

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
