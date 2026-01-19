"""
AWS Batch tests using moto (free, no LocalStack Pro needed).

Moto mocks AWS APIs in-memory without Docker overhead (unless use_docker=True).
This is ideal for unit/integration testing Batch job definitions and workflows.

Run with: uv run pytest tests/test_batch_moto.py -v
"""

import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def aws_credentials():
    """Set up mock AWS credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def batch_client(aws_credentials):
    """Boto3 Batch client with moto mock."""
    with mock_aws():
        yield boto3.client("batch", region_name="us-east-1")


@pytest.fixture
def iam_client(aws_credentials):
    """Boto3 IAM client with moto mock."""
    with mock_aws():
        yield boto3.client("iam", region_name="us-east-1")


@pytest.fixture
def ec2_client(aws_credentials):
    """Boto3 EC2 client with moto mock."""
    with mock_aws():
        yield boto3.client("ec2", region_name="us-east-1")


@pytest.fixture
def batch_setup(aws_credentials):
    """
    Complete Batch setup fixture with all dependencies.

    Creates: VPC, subnet, security group, IAM roles, compute env, job queue.
    Yields a dict with all created resource references.
    """
    with mock_aws():
        ec2 = boto3.client("ec2", region_name="us-east-1")
        iam = boto3.client("iam", region_name="us-east-1")
        batch = boto3.client("batch", region_name="us-east-1")

        # Create VPC and subnet (required for compute environment)
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]

        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]

        sg = ec2.create_security_group(
            GroupName="batch-sg",
            Description="Security group for Batch",
            VpcId=vpc_id,
        )
        sg_id = sg["GroupId"]

        # Create IAM roles
        trust_policy = """{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "batch.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }"""

        iam.create_role(
            RoleName="BatchServiceRole",
            AssumeRolePolicyDocument=trust_policy,
        )
        service_role_arn = "arn:aws:iam::123456789012:role/BatchServiceRole"

        iam.create_role(
            RoleName="ecsInstanceRole",
            AssumeRolePolicyDocument=trust_policy,
        )

        # Create instance profile for EC2 instances
        resp = iam.create_instance_profile(InstanceProfileName="ecsInstanceRole")
        instance_profile_arn = resp["InstanceProfile"]["Arn"]
        iam.add_role_to_instance_profile(
            InstanceProfileName="ecsInstanceRole",
            RoleName="ecsInstanceRole",
        )

        # Create compute environment
        batch.create_compute_environment(
            computeEnvironmentName="test-compute-env",
            type="MANAGED",
            state="ENABLED",
            serviceRole=service_role_arn,
            computeResources={
                "type": "EC2",
                "minvCpus": 0,
                "maxvCpus": 4,
                "desiredvCpus": 0,
                "instanceTypes": ["optimal"],
                "subnets": [subnet_id],
                "securityGroupIds": [sg_id],
                "instanceRole": instance_profile_arn,
            },
        )

        # Create job queue
        batch.create_job_queue(
            jobQueueName="test-job-queue",
            state="ENABLED",
            priority=1,
            computeEnvironmentOrder=[{"order": 1, "computeEnvironment": "test-compute-env"}],
        )

        yield {
            "batch": batch,
            "iam": iam,
            "ec2": ec2,
            "vpc_id": vpc_id,
            "subnet_id": subnet_id,
            "sg_id": sg_id,
            "compute_env": "test-compute-env",
            "job_queue": "test-job-queue",
            "service_role_arn": service_role_arn,
        }


def test_create_job_definition(batch_setup):
    """Test registering a job definition."""
    batch = batch_setup["batch"]

    response = batch.register_job_definition(
        jobDefinitionName="tickers-pipeline",
        type="container",
        containerProperties={
            "image": "python:3.12-slim",
            "vcpus": 1,
            "memory": 512,
            "command": ["python", "-m", "src.pipelines.tickers.main"],
            "environment": [
                {"name": "TABLE_BUCKET_ARN", "value": "arn:aws:s3tables:us-east-1:123:bucket/test"},
            ],
        },
    )

    assert response["jobDefinitionName"] == "tickers-pipeline"
    assert response["revision"] == 1

    # Verify we can describe it
    desc = batch.describe_job_definitions(jobDefinitionName="tickers-pipeline")
    assert len(desc["jobDefinitions"]) == 1
    assert desc["jobDefinitions"][0]["containerProperties"]["image"] == "python:3.12-slim"


def test_submit_and_describe_job(batch_setup):
    """Test submitting a job and checking its status."""
    batch = batch_setup["batch"]

    # Register job definition first
    batch.register_job_definition(
        jobDefinitionName="test-job",
        type="container",
        containerProperties={
            "image": "alpine:latest",
            "vcpus": 1,
            "memory": 128,
            "command": ["echo", "hello"],
        },
    )

    # Submit job
    response = batch.submit_job(
        jobName="test-job-1",
        jobQueue=batch_setup["job_queue"],
        jobDefinition="test-job",
    )

    job_id = response["jobId"]
    assert job_id is not None

    # Describe job
    desc = batch.describe_jobs(jobs=[job_id])
    assert len(desc["jobs"]) == 1
    assert desc["jobs"][0]["jobName"] == "test-job-1"


def test_job_with_environment_overrides(batch_setup):
    """Test job submission with environment variable overrides."""
    batch = batch_setup["batch"]

    batch.register_job_definition(
        jobDefinitionName="env-test-job",
        type="container",
        containerProperties={
            "image": "alpine:latest",
            "vcpus": 1,
            "memory": 128,
            "command": ["printenv"],
            "environment": [
                {"name": "DEFAULT_VAR", "value": "default"},
            ],
        },
    )

    # Submit with overrides
    response = batch.submit_job(
        jobName="env-test-1",
        jobQueue=batch_setup["job_queue"],
        jobDefinition="env-test-job",
        containerOverrides={
            "environment": [
                {"name": "OVERRIDE_VAR", "value": "custom"},
            ],
        },
    )

    assert response["jobId"] is not None
