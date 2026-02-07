"""
CDK assertion tests for the ticker prices (OHLC) pipeline stack.

Tests verify that synthesized CloudFormation templates contain expected
resources with correct configurations. This catches misconfigurations
before deployment.

Run with: uv run pytest cdk/tests/ -v
"""

import pytest
from aws_cdk import App, Environment
from aws_cdk.assertions import Match, Template
from stacks.pipelines.ticker_prices import TickerPricesPipelineStack


@pytest.fixture
def template():
    """Synthesize stack and return Template for assertions."""
    app = App()
    # Use dummy account/region for VPC lookup during synth
    env = Environment(account="123456789012", region="us-east-1")
    stack = TickerPricesPipelineStack(app, "test-ticker-prices-pipeline", env=env)
    return Template.from_stack(stack)


class TestBatchCompute:
    """Tests for Batch compute environment and job queue."""

    def test_creates_compute_environment(self, template):
        """Compute environment is created."""
        template.resource_count_is("AWS::Batch::ComputeEnvironment", 1)

    def test_creates_job_queue(self, template):
        """Job queue is created with correct name."""
        template.has_resource_properties(
            "AWS::Batch::JobQueue",
            {"JobQueueName": "petals-ticker-prices-queue"},
        )

    def test_creates_job_definition(self, template):
        """Job definition is created with correct name."""
        template.has_resource_properties(
            "AWS::Batch::JobDefinition",
            {"JobDefinitionName": "petals-ticker-prices-job"},
        )

    def test_job_has_timeout(self, template):
        """Job definition has timeout configured (30 min for ~15k tickers)."""
        template.has_resource_properties(
            "AWS::Batch::JobDefinition",
            {
                "Timeout": {"AttemptDurationSeconds": 1800},  # 30 min
            },
        )

    def test_job_has_pipeline_env_var(self, template):
        """Job definition specifies ticker_prices pipeline module."""
        template.has_resource_properties(
            "AWS::Batch::JobDefinition",
            {
                "ContainerProperties": {
                    "Environment": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Name": "PIPELINE",
                                    "Value": "src.pipelines.ticker_prices.main",
                                }
                            )
                        ]
                    ),
                },
            },
        )


class TestStepFunctions:
    """Tests for Step Functions state machine."""

    def test_creates_state_machine(self, template):
        """State machine is created with correct name."""
        template.has_resource_properties(
            "AWS::StepFunctions::StateMachine",
            {"StateMachineName": "petals-ticker-prices-pipeline"},
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
            {"Name": "petals-ticker-prices-daily"},
        )

    def test_schedule_is_weekdays_7am_utc(self, template):
        """Schedule runs weekdays at 7 AM UTC (8 AM Stockholm time)."""
        template.has_resource_properties(
            "AWS::Events::Rule",
            {"ScheduleExpression": "cron(0 7 ? * MON-FRI *)"},
        )


class TestIAMPermissions:
    """Tests for IAM roles and permissions."""

    def test_job_role_exists(self, template):
        """Job execution role is created."""
        template.has_resource_properties(
            "AWS::IAM::Role",
            {"RoleName": "petals-ticker-prices-job-role"},
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

    def test_no_secrets_required(self, template):
        """Ticker prices pipeline should NOT have Secrets Manager access (yfinance is free)."""
        # Verify no secrets are configured in the container
        template.has_resource_properties(
            "AWS::Batch::JobDefinition",
            {
                "ContainerProperties": {
                    # Secrets array should not exist or be empty
                    "Secrets": Match.absent(),
                },
            },
        )
