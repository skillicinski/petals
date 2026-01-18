"""
Verify LocalStack setup is working.

Run with: uv run pytest tests/test_localstack_setup.py -v

Note: AWS Batch requires LocalStack Pro (paid). These tests use S3 and other
free-tier services for verification.
"""

import pytest


def test_localstack_is_running(localstack):
    """Verify LocalStack container starts successfully."""
    # If we get here, the fixture worked
    assert localstack.get_url().startswith("http://")


def test_s3_operations(s3_client):
    """Test basic S3 operations against LocalStack."""
    bucket_name = "test-bucket"

    # Create bucket
    s3_client.create_bucket(Bucket=bucket_name)

    # List buckets
    response = s3_client.list_buckets()
    bucket_names = [b["Name"] for b in response["Buckets"]]
    assert bucket_name in bucket_names

    # Put and get object
    s3_client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"hello world")
    response = s3_client.get_object(Bucket=bucket_name, Key="test.txt")
    assert response["Body"].read() == b"hello world"


@pytest.mark.skip(reason="AWS Batch requires LocalStack Pro (paid)")
def test_batch_compute_environment(batch_client, iam_client):
    """Test creating a Batch compute environment in LocalStack.

    NOTE: This test requires LocalStack Pro. Skip in free tier.
    To enable, set LOCALSTACK_AUTH_TOKEN env var with your Pro token.
    """
    # Create a minimal IAM role for Batch
    role_name = "batch-service-role"
    try:
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument="""{
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "batch.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }]
            }""",
        )
    except iam_client.exceptions.EntityAlreadyExistsException:
        pass

    role_arn = f"arn:aws:iam::000000000000:role/{role_name}"

    # Create compute environment
    ce_name = "test-compute-env"
    batch_client.create_compute_environment(
        computeEnvironmentName=ce_name,
        type="MANAGED",
        state="ENABLED",
        serviceRole=role_arn,
        computeResources={
            "type": "EC2",
            "minvCpus": 0,
            "maxvCpus": 4,
            "desiredvCpus": 0,
            "instanceTypes": ["optimal"],
            "subnets": ["subnet-12345"],
            "securityGroupIds": ["sg-12345"],
        },
    )

    # Verify it was created
    response = batch_client.describe_compute_environments(computeEnvironments=[ce_name])
    assert len(response["computeEnvironments"]) == 1
    assert response["computeEnvironments"][0]["computeEnvironmentName"] == ce_name
