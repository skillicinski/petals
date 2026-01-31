"""
CDK assertion tests for the ticker details pipeline stack.

Tests verify that synthesized CloudFormation templates contain expected
resources with correct configurations. This catches misconfigurations
before deployment.

Run with: uv run pytest cdk/tests/ -v
"""

import pytest
from aws_cdk import App, Environment
from aws_cdk.assertions import Match, Template
from stacks.pipelines.ticker_details import TickerDetailsPipelineStack


@pytest.fixture
def template():
    """Synthesize stack and return Template for assertions."""
    app = App()
    env = Environment(account='123456789012', region='us-east-1')
    stack = TickerDetailsPipelineStack(app, 'test-ticker-details-pipeline', env=env)
    return Template.from_stack(stack)


class TestBatchCompute:
    """Tests for Batch compute environment and job queue."""

    def test_creates_compute_environment(self, template):
        """Compute environment is created."""
        template.resource_count_is('AWS::Batch::ComputeEnvironment', 1)

    def test_creates_job_queue(self, template):
        """Job queue is created with correct name."""
        template.has_resource_properties(
            'AWS::Batch::JobQueue',
            {'JobQueueName': 'petals-ticker-details-queue'},
        )

    def test_creates_job_definition(self, template):
        """Job definition is created with correct name."""
        template.has_resource_properties(
            'AWS::Batch::JobDefinition',
            {'JobDefinitionName': 'petals-ticker-details-job'},
        )

    def test_job_has_long_timeout(self, template):
        """Job definition has long timeout for full backfill (6 days)."""
        template.has_resource_properties(
            'AWS::Batch::JobDefinition',
            {
                'Timeout': {'AttemptDurationSeconds': 518400},  # 6 days
            },
        )

    def test_job_has_pipeline_env_var(self, template):
        """Job definition specifies ticker_details pipeline module."""
        template.has_resource_properties(
            'AWS::Batch::JobDefinition',
            {
                'ContainerProperties': {
                    'Environment': Match.array_with(
                        [
                            Match.object_like(
                                {
                                    'Name': 'PIPELINE',
                                    'Value': 'src.pipelines.ticker_details.main',
                                }
                            )
                        ]
                    ),
                },
            },
        )

    def test_job_has_adequate_memory(self, template):
        """Job definition has 2GB memory for batch processing."""
        template.has_resource_properties(
            'AWS::Batch::JobDefinition',
            {
                'ContainerProperties': {
                    'ResourceRequirements': Match.array_with(
                        [
                            Match.object_like(
                                {
                                    'Type': 'MEMORY',
                                    'Value': '2048',
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
            'AWS::StepFunctions::StateMachine',
            {'StateMachineName': 'petals-ticker-details-pipeline'},
        )

    def test_has_xray_tracing(self, template):
        """State machine has X-Ray tracing enabled."""
        template.has_resource_properties(
            'AWS::StepFunctions::StateMachine',
            {
                'TracingConfiguration': {'Enabled': True},
            },
        )

    def test_has_error_handling(self, template):
        """State machine definition includes error handling (Catch block)."""
        template.has_resource_properties(
            'AWS::StepFunctions::StateMachine',
            {
                'DefinitionString': {
                    'Fn::Join': Match.array_with(
                        [
                            '',
                            Match.array_with(
                                [
                                    Match.string_like_regexp(r'Catch'),
                                ]
                            ),
                        ]
                    ),
                },
            },
        )

    def test_has_concurrency_control(self, template):
        """State machine checks for running flag to prevent concurrent executions."""
        template.has_resource_properties(
            'AWS::StepFunctions::StateMachine',
            {
                'DefinitionString': {
                    'Fn::Join': Match.array_with(
                        [
                            '',
                            Match.array_with(
                                [
                                    Match.string_like_regexp(r'SkipAlreadyRunning'),
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
            'AWS::Events::Rule',
            {'Name': 'petals-ticker-details-daily'},
        )

    def test_schedule_is_daily_7am_utc(self, template):
        """Schedule runs daily at 7 AM UTC (1 hour after tickers)."""
        template.has_resource_properties(
            'AWS::Events::Rule',
            {'ScheduleExpression': 'cron(0 7 ? * * *)'},
        )


class TestIAMPermissions:
    """Tests for IAM roles and permissions."""

    def test_job_role_exists(self, template):
        """Job execution role is created."""
        template.has_resource_properties(
            'AWS::IAM::Role',
            {'RoleName': 'petals-ticker-details-job-role'},
        )

    def test_job_role_has_scoped_s3tables_access(self, template):
        """Job role has S3 Tables permissions scoped to table bucket."""
        template.has_resource_properties(
            'AWS::IAM::Policy',
            {
                'PolicyDocument': {
                    'Statement': Match.array_with(
                        [
                            Match.object_like(
                                {
                                    'Action': 's3tables:*',
                                    'Effect': 'Allow',
                                    'Resource': Match.not_(Match.exact('*')),
                                }
                            )
                        ]
                    ),
                },
            },
        )


class TestLogging:
    """Tests for CloudWatch logging configuration."""

    def test_creates_log_group(self, template):
        """Log group is created with correct name."""
        template.has_resource_properties(
            'AWS::Logs::LogGroup',
            {'LogGroupName': '/petals/pipelines/ticker_details'},
        )

    def test_log_retention_two_weeks(self, template):
        """Log retention is set to 2 weeks."""
        template.has_resource_properties(
            'AWS::Logs::LogGroup',
            {'RetentionInDays': 14},
        )
