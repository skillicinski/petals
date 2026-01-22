"""
CDK assertion tests for the tickers pipeline stack.

Tests verify that synthesized CloudFormation templates contain expected
resources with correct configurations. This catches misconfigurations
before deployment.

Run with: uv run pytest cdk/tests/ -v
"""

import pytest
from aws_cdk import App, Environment
from aws_cdk.assertions import Match, Template
from stacks.pipelines.tickers import TickersPipelineStack


@pytest.fixture
def template():
    """Synthesize stack and return Template for assertions."""
    app = App()
    # Use dummy account/region for VPC lookup during synth
    env = Environment(account="123456789012", region="us-east-1")
    stack = TickersPipelineStack(app, "test-tickers-pipeline", env=env)
    return Template.from_stack(stack)


class TestECRRepository:
    """Tests for pipeline container ECR repository."""

    def test_creates_ecr_repo(self, template):
        """ECR repository is created with correct name."""
        template.has_resource_properties(
            "AWS::ECR::Repository",
            {"RepositoryName": "petals-tickers-pipeline"},
        )

    def test_has_lifecycle_policy(self, template):
        """ECR repo has lifecycle rule to limit image count."""
        template.has_resource_properties(
            "AWS::ECR::Repository",
            {
                "LifecyclePolicy": {
                    "LifecyclePolicyText": Match.string_like_regexp(r'"countNumber":\s*5'),
                },
            },
        )


class TestBatchCompute:
    """Tests for Batch compute environment and job queue."""

    def test_creates_compute_environment(self, template):
        """Compute environment is created."""
        template.resource_count_is("AWS::Batch::ComputeEnvironment", 1)

    def test_creates_job_queue(self, template):
        """Job queue is created with correct name."""
        template.has_resource_properties(
            "AWS::Batch::JobQueue",
            {"JobQueueName": "petals-tickers-queue"},
        )

    def test_creates_job_definition(self, template):
        """Job definition is created with correct name."""
        template.has_resource_properties(
            "AWS::Batch::JobDefinition",
            {"JobDefinitionName": "petals-tickers-job"},
        )

    def test_job_has_timeout(self, template):
        """Job definition has timeout configured."""
        template.has_resource_properties(
            "AWS::Batch::JobDefinition",
            {
                "Timeout": {"AttemptDurationSeconds": 3600},  # 60 min
            },
        )


class TestStepFunctions:
    """Tests for Step Functions state machine."""

    def test_creates_state_machine(self, template):
        """State machine is created with correct name."""
        template.has_resource_properties(
            "AWS::StepFunctions::StateMachine",
            {"StateMachineName": "petals-tickers-pipeline"},
        )

    def test_has_xray_tracing(self, template):
        """State machine has X-Ray tracing enabled."""
        template.has_resource_properties(
            "AWS::StepFunctions::StateMachine",
            {
                "TracingConfiguration": {"Enabled": True},
            },
        )

    def test_has_error_handling(self, template):
        """State machine definition includes error handling (Catch block)."""
        # The state machine definition contains Fn::Join with parts that include "Catch"
        # We verify by checking the definition structure contains our error handling states
        template.has_resource_properties(
            "AWS::StepFunctions::StateMachine",
            {
                "DefinitionString": {
                    "Fn::Join": Match.array_with(
                        [
                            "",  # Join delimiter
                            Match.array_with(
                                [
                                    Match.string_like_regexp(r"Catch"),
                                ]
                            ),
                        ]
                    ),
                },
            },
        )


class TestEventBridgeSchedule:
    """Tests for EventBridge scheduling rule."""

    def test_creates_schedule_rule(self, template):
        """Schedule rule is created with correct name."""
        template.has_resource_properties(
            "AWS::Events::Rule",
            {"Name": "petals-tickers-daily"},
        )

    def test_schedule_is_daily_6am_utc(self, template):
        """Schedule runs daily at 6 AM UTC."""
        template.has_resource_properties(
            "AWS::Events::Rule",
            {"ScheduleExpression": "cron(0 6 ? * * *)"},
        )


class TestIAMPermissions:
    """Tests for IAM roles and permissions."""

    def test_job_role_exists(self, template):
        """Job execution role is created."""
        template.has_resource_properties(
            "AWS::IAM::Role",
            {"RoleName": "petals-tickers-job-role"},
        )

    def test_job_role_has_scoped_s3tables_access(self, template):
        """Job role has S3 Tables permissions scoped to table bucket (not wildcard)."""
        template.has_resource_properties(
            "AWS::IAM::Policy",
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Action": "s3tables:*",
                                    "Effect": "Allow",
                                    # Resources should NOT be '*' (wildcard)
                                    "Resource": Match.not_(Match.exact("*")),
                                }
                            )
                        ]
                    ),
                },
            },
        )
