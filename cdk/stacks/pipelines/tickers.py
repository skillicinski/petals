"""
CDK stack for the tickers pipeline.

Components:
- DynamoDB table for pipeline state (last run timestamp)
- Batch compute environment (Fargate Spot for cost optimization)
- Batch job queue and job definition
- Step Functions state machine for orchestration
- EventBridge rule for daily scheduling
"""

from aws_cdk import (
    CfnOutput,
    Duration,
    Fn,
    RemovalPolicy,
    Size,
    Stack,
)
from aws_cdk import aws_batch as batch
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class TickersPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Import shared resources
        table_bucket_arn = Fn.import_value("petals-table-bucket-arn")
        state_table_arn = Fn.import_value("petals-pipeline-state-table-arn")

        # =================================================================
        # State Storage (imported from SharedStack)
        # =================================================================
        self.state_table = dynamodb.Table.from_table_arn(
            self,
            "PipelineState",
            state_table_arn,
        )

        # =================================================================
        # Secrets (API Key)
        # =================================================================
        # Reference existing secret (created manually to avoid key in code)
        self.api_secret = secretsmanager.Secret.from_secret_name_v2(
            self,
            "MassiveApiKey",
            "petals/Massive",
        )

        # =================================================================
        # ECR Repository
        # =================================================================
        self.ecr_repo = ecr.Repository(
            self,
            "TickersPipelineRepo",
            repository_name="petals-tickers-pipeline",
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                ecr.LifecycleRule(
                    max_image_count=5,
                    description="Keep only 5 images",
                )
            ],
        )

        # =================================================================
        # VPC (use default VPC for cost savings - no NAT Gateway)
        # =================================================================
        self.vpc = ec2.Vpc.from_lookup(
            self,
            "DefaultVpc",
            is_default=True,
        )

        # =================================================================
        # Batch Compute Environment (Fargate Spot)
        # =================================================================
        self.compute_env = batch.FargateComputeEnvironment(
            self,
            "TickersComputeEnv",
            compute_environment_name="petals-tickers-compute",
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            spot=True,  # Use Spot for cost savings (up to 70% off)
            maxv_cpus=4,  # Limit max concurrent vCPUs
        )

        # =================================================================
        # Batch Job Queue
        # =================================================================
        self.job_queue = batch.JobQueue(
            self,
            "TickersJobQueue",
            job_queue_name="petals-tickers-queue",
            compute_environments=[
                batch.OrderedComputeEnvironment(
                    compute_environment=self.compute_env,
                    order=1,
                )
            ],
        )

        # =================================================================
        # Batch Job Role (for the container)
        # =================================================================
        self.job_role = iam.Role(
            self,
            "TickersJobRole",
            role_name="petals-tickers-job-role",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        # S3 Tables access - scoped to our table bucket
        self.job_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3tables:*"],
                resources=[
                    table_bucket_arn,
                    f"{table_bucket_arn}/*",
                ],
            )
        )

        # Secret access
        self.api_secret.grant_read(self.job_role)

        # =================================================================
        # Batch Job Definition
        # =================================================================
        self.job_definition = batch.EcsJobDefinition(
            self,
            "TickersJobDef",
            job_definition_name="petals-tickers-job",
            container=batch.EcsFargateContainerDefinition(
                self,
                "TickersContainer",
                image=ecs.ContainerImage.from_ecr_repository(self.ecr_repo, "latest"),
                cpu=0.5,
                memory=Size.mebibytes(1024),  # 1 GB
                job_role=self.job_role,
                logging=ecs.LogDriver.aws_logs(
                    stream_prefix="tickers-pipeline",
                    log_group=logs.LogGroup(
                        self,
                        "TickersLogGroup",
                        log_group_name="/petals/pipelines/tickers",
                        retention=logs.RetentionDays.TWO_WEEKS,
                        removal_policy=RemovalPolicy.DESTROY,
                    ),
                ),
                environment={
                    "TABLE_BUCKET_ARN": table_bucket_arn,
                    "AWS_DEFAULT_REGION": self.region,
                },
                secrets={
                    "MASSIVE_API_KEY": batch.Secret.from_secrets_manager(
                        self.api_secret, "Default"
                    ),
                },
                assign_public_ip=True,  # Required to reach Secrets Manager/ECR without NAT
            ),
            timeout=Duration.minutes(30),
            retry_attempts=1,
        )

        # =================================================================
        # Step Functions State Machine
        # =================================================================

        # Task: Read last run time from DynamoDB
        get_state = tasks.DynamoGetItem(
            self,
            "GetPipelineState",
            table=self.state_table,
            key={"pipeline_id": tasks.DynamoAttributeValue.from_string("tickers")},
            result_path="$.state",
        )

        # Task: Run Batch job
        run_batch = tasks.BatchSubmitJob(
            self,
            "RunTickersPipeline",
            job_definition_arn=self.job_definition.job_definition_arn,
            job_queue_arn=self.job_queue.job_queue_arn,
            job_name="tickers-pipeline",
            container_overrides=tasks.BatchContainerOverrides(
                environment={
                    "LAST_RUN_TIME": sfn.JsonPath.string_at(
                        "States.Format('{}', $.state.Item.last_run_time.S)"
                    ),
                    "FORCE_FULL": sfn.JsonPath.string_at("States.Format('{}', $.force_full)"),
                },
            ),
            result_path="$.batch_result",
        )

        # Task: Update state with new run time (success)
        update_state = tasks.DynamoPutItem(
            self,
            "UpdatePipelineState",
            table=self.state_table,
            item={
                "pipeline_id": tasks.DynamoAttributeValue.from_string("tickers"),
                "last_run_time": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.State.EnteredTime")
                ),
                "last_status": tasks.DynamoAttributeValue.from_string("SUCCESS"),
            },
            result_path="$.update_result",
        )

        # Task: Record failure state (for Spot interruption, OOM, etc.)
        record_failure = tasks.DynamoPutItem(
            self,
            "RecordFailure",
            table=self.state_table,
            item={
                "pipeline_id": tasks.DynamoAttributeValue.from_string("tickers"),
                "last_status": tasks.DynamoAttributeValue.from_string("FAILED"),
                "error": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.error.Error")
                ),
                "cause": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.error.Cause")
                ),
            },
            result_path="$.failure_recorded",
        )

        # Fail state after recording
        fail_state = sfn.Fail(
            self,
            "PipelineFailed",
            error="BatchJobFailed",
            cause="Batch job failed - see DynamoDB for details",
        )
        record_failure.next(fail_state)

        # Add error handling to Batch job
        run_batch.add_catch(
            record_failure,
            errors=["States.ALL"],
            result_path="$.error",
        )

        # Handle missing state (first run) or missing last_run_time (previous failure)
        check_state = sfn.Choice(self, "CheckStateExists")
        set_defaults = sfn.Pass(
            self,
            "SetDefaults",
            parameters={
                "state": {
                    "Item": {
                        "last_run_time": {"S": ""},
                    }
                },
                "force_full": sfn.JsonPath.string_at("States.Format('{}', $.force_full)"),
            },
        )

        # Pass through existing state
        pass_state = sfn.Pass(
            self,
            "PassState",
            parameters={
                "state": sfn.JsonPath.object_at("$.state"),
                "force_full": sfn.JsonPath.string_at("States.Format('{}', $.force_full)"),
            },
        )

        # Build state machine definition
        # Check for missing Item (first run) OR missing last_run_time (previous failure)
        definition = get_state.next(
            check_state.when(
                sfn.Condition.or_(
                    sfn.Condition.is_not_present("$.state.Item"),
                    sfn.Condition.is_not_present("$.state.Item.last_run_time"),
                ),
                set_defaults.next(run_batch),
            ).otherwise(pass_state.next(run_batch))
        )
        run_batch.next(update_state)

        self.state_machine = sfn.StateMachine(
            self,
            "TickersPipelineStateMachine",
            state_machine_name="petals-tickers-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.minutes(45),
            tracing_enabled=True,
        )

        # Grant state machine permissions
        self.state_table.grant_read_write_data(self.state_machine)

        # =================================================================
        # EventBridge Schedule (daily at 6 AM UTC)
        # =================================================================
        self.schedule_rule = events.Rule(
            self,
            "TickersSchedule",
            rule_name="petals-tickers-daily",
            schedule=events.Schedule.cron(
                minute="0",
                hour="6",
                month="*",
                week_day="*",
                year="*",
            ),
            targets=[
                targets.SfnStateMachine(
                    self.state_machine,
                    input=events.RuleTargetInput.from_object({"force_full": False}),
                )
            ],
        )

        # =================================================================
        # Outputs
        # =================================================================
        CfnOutput(
            self,
            "ECRRepositoryUri",
            value=self.ecr_repo.repository_uri,
            description="ECR repository URI for tickers pipeline image",
        )

        CfnOutput(
            self,
            "StateMachineArn",
            value=self.state_machine.state_machine_arn,
            description="Step Functions state machine ARN",
        )

        CfnOutput(
            self,
            "JobQueueArn",
            value=self.job_queue.job_queue_arn,
            description="Batch job queue ARN",
        )
