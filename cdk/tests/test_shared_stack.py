"""
CDK assertion tests for the shared infrastructure stack.

Run with: uv run pytest cdk/tests/test_shared_stack.py -v
"""

import pytest
from aws_cdk import App
from aws_cdk.assertions import Template
from stacks.shared import SharedStack


@pytest.fixture
def template():
    """Synthesize stack and return Template for assertions."""
    app = App()
    stack = SharedStack(app, "test-shared")
    return Template.from_stack(stack)


class TestPipelineStateTable:
    """Tests for shared pipeline state DynamoDB table."""

    def test_creates_state_table(self, template):
        """State table is created with correct name and key."""
        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {
                "TableName": "petals-pipeline-state",
                "KeySchema": [
                    {"AttributeName": "pipeline_id", "KeyType": "HASH"},
                ],
            },
        )

    def test_uses_pay_per_request_billing(self, template):
        """State table uses on-demand billing for cost efficiency."""
        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {"BillingMode": "PAY_PER_REQUEST"},
        )

    def test_table_retained_on_delete(self, template):
        """State table is retained when stack is deleted (data protection)."""
        template.has_resource(
            "AWS::DynamoDB::Table",
            {
                "DeletionPolicy": "Retain",
                "UpdateReplacePolicy": "Retain",
            },
        )

    def test_exports_table_name(self, template):
        """Table name is exported for cross-stack reference."""
        template.has_output(
            "PipelineStateTableName",
            {
                "Export": {"Name": "petals-pipeline-state-table-name"},
            },
        )

    def test_exports_table_arn(self, template):
        """Table ARN is exported for cross-stack reference."""
        template.has_output(
            "PipelineStateTableArn",
            {
                "Export": {"Name": "petals-pipeline-state-table-arn"},
            },
        )


class TestS3TableBucket:
    """Tests for S3 Table Bucket (Iceberg storage)."""

    def test_creates_table_bucket(self, template):
        """S3 Table Bucket is created."""
        template.resource_count_is("AWS::S3Tables::TableBucket", 1)

    def test_exports_bucket_arn(self, template):
        """Bucket ARN is exported for cross-stack reference."""
        template.has_output(
            "TableBucketArn",
            {
                "Export": {"Name": "petals-table-bucket-arn"},
            },
        )
