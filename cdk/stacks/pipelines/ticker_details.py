"""
CDK stack for the ticker details enrichment pipeline.

Components:
- Batch compute environment (Fargate On-Demand for reliability)
- Batch job queue and job definition
- Step Functions state machine for orchestration (with DynamoDB lock)
- EventBridge rule for daily scheduling (1 hour after tickers pipeline)

Concurrency Control:
- Uses a DynamoDB 'running' flag to prevent concurrent executions
- If an execution is already running, new executions skip gracefully
- Lock is released on both success and failure

Note: Initial backfill for ~35k US stock tickers takes ~123 hours due to
API rate limiting. Subsequent runs only process new/changed tickers.
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


class TickerDetailsPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Import shared resources
        table_bucket_arn = Fn.import_value("petals-table-bucket-arn")
        state_table_arn = Fn.import_value("petals-pipeline-state-table-arn")
        ecr_repo_arn = Fn.import_value("petals-ecr-repo-arn")

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
        self.api_secret = secretsmanager.Secret.from_secret_name_v2(
            self,
            "MassiveApiKey",
            "petals/Massive",
        )

        # =================================================================
        # ECR Repository (imported from SharedStack)
        # =================================================================
        self.ecr_repo = ecr.Repository.from_repository_attributes(
            self,
            "PipelinesRepo",
            repository_arn=ecr_repo_arn,
            repository_name="petals-pipelines",
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
        # Batch Compute Environment (On-Demand for reliability)
        # Long-running job (~8+ hours), Spot interruption risk too high
        # =================================================================
        self.compute_env = batch.FargateComputeEnvironment(
            self,
            "TickerDetailsComputeEnv",
            compute_environment_name="petals-ticker-details-compute",
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            spot=False,  # On-Demand - only ~$0.16 more for full backfill
            maxv_cpus=4,
        )

        # =================================================================
        # Batch Job Queue
        # =================================================================
        self.job_queue = batch.JobQueue(
            self,
            "TickerDetailsJobQueue",
            job_queue_name="petals-ticker-details-queue",
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
            "TickerDetailsJobRole",
            role_name="petals-ticker-details-job-role",
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
        # Long timeout for initial backfill (~8+ hours due to rate limiting)
        # =================================================================
        self.job_definition = batch.EcsJobDefinition(
            self,
            "TickerDetailsJobDef",
            job_definition_name="petals-ticker-details-job",
            container=batch.EcsFargateContainerDefinition(
                self,
                "TickerDetailsContainer",
                image=ecs.ContainerImage.from_ecr_repository(self.ecr_repo, "latest"),
                cpu=0.5,
                memory=Size.mebibytes(2048),  # 2 GB for processing batches
                job_role=self.job_role,
                logging=ecs.LogDriver.aws_logs(
                    stream_prefix="ticker-details-pipeline",
                    log_group=logs.LogGroup(
                        self,
                        "TickerDetailsLogGroup",
                        log_group_name="/petals/pipelines/ticker_details",
                        retention=logs.RetentionDays.TWO_WEEKS,
                        removal_policy=RemovalPolicy.DESTROY,
                    ),
                ),
                environment={
                    "TABLE_BUCKET_ARN": table_bucket_arn,
                    "AWS_DEFAULT_REGION": self.region,
                    "PIPELINE": "src.pipelines.ticker_details.main",
                },
                secrets={
                    "MASSIVE_API_KEY": batch.Secret.from_secrets_manager(
                        self.api_secret, "Default"
                    ),
                },
                assign_public_ip=True,
            ),
            timeout=Duration.days(6),  # ~144 hours for full US stock backfill
            retry_attempts=1,
        )

        # =================================================================
        # Step Functions State Machine
        # Uses DynamoDB lock to prevent concurrent executions
        # =================================================================

        # Task: Read pipeline state (includes running lock)
        get_state = tasks.DynamoGetItem(
            self,
            "GetPipelineState",
            table=self.state_table,
            key={"pipeline_id": tasks.DynamoAttributeValue.from_string("ticker_details")},
            result_path="$.state",
        )

        # Skip if already running
        skip_already_running = sfn.Succeed(
            self,
            "SkipAlreadyRunning",
            comment="Another execution is already running - skipping",
        )

        # Task: Acquire lock (set running=true)
        acquire_lock = tasks.DynamoUpdateItem(
            self,
            "AcquireLock",
            table=self.state_table,
            key={"pipeline_id": tasks.DynamoAttributeValue.from_string("ticker_details")},
            update_expression="SET running = :running",
            expression_attribute_values={
                ":running": tasks.DynamoAttributeValue.from_boolean(True),
            },
            result_path="$.lock_result",
        )

        # Task: Run Batch job (no more FORCE_FULL/LAST_RUN_TIME - handled in code)
        run_batch = tasks.BatchSubmitJob(
            self,
            "RunTickerDetailsPipeline",
            job_definition_arn=self.job_definition.job_definition_arn,
            job_queue_arn=self.job_queue.job_queue_arn,
            job_name="ticker-details-pipeline",
            result_path="$.batch_result",
        )

        # Task: Release lock and update state (success)
        release_lock_success = tasks.DynamoUpdateItem(
            self,
            "ReleaseLockSuccess",
            table=self.state_table,
            key={"pipeline_id": tasks.DynamoAttributeValue.from_string("ticker_details")},
            update_expression="SET running = :running, last_run_time = :time, last_status = :status",
            expression_attribute_values={
                ":running": tasks.DynamoAttributeValue.from_boolean(False),
                ":time": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.State.EnteredTime")
                ),
                ":status": tasks.DynamoAttributeValue.from_string("SUCCESS"),
            },
            result_path="$.update_result",
        )

        # Task: Release lock and record failure
        release_lock_failure = tasks.DynamoUpdateItem(
            self,
            "ReleaseLockFailure",
            table=self.state_table,
            key={"pipeline_id": tasks.DynamoAttributeValue.from_string("ticker_details")},
            update_expression="SET running = :running, last_status = :status, #err = :error, cause = :cause",
            expression_attribute_names={"#err": "error"},
            expression_attribute_values={
                ":running": tasks.DynamoAttributeValue.from_boolean(False),
                ":status": tasks.DynamoAttributeValue.from_string("FAILED"),
                ":error": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.error.Error")
                ),
                ":cause": tasks.DynamoAttributeValue.from_string(
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
        release_lock_failure.next(fail_state)

        # Add error handling to Batch job
        run_batch.add_catch(
            release_lock_failure,
            errors=["States.ALL"],
            result_path="$.error",
        )

        # Check if already running
        check_running = sfn.Choice(self, "CheckIfRunning")

        # Build state machine definition
        definition = get_state.next(
            check_running.when(
                sfn.Condition.boolean_equals("$.state.Item.running.BOOL", True),
                skip_already_running,
            ).otherwise(acquire_lock.next(run_batch))
        )
        run_batch.next(release_lock_success)

        self.state_machine = sfn.StateMachine(
            self,
            "TickerDetailsPipelineStateMachine",
            state_machine_name="petals-ticker-details-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.days(7),  # Slightly longer than job timeout
            tracing_enabled=True,
        )

        # Grant state machine permissions
        self.state_table.grant_read_write_data(self.state_machine)

        # =================================================================
        # EventBridge Schedule (daily at 7 AM UTC, 1 hour after tickers)
        # =================================================================
        self.schedule_rule = events.Rule(
            self,
            "TickerDetailsSchedule",
            rule_name="petals-ticker-details-daily",
            schedule=events.Schedule.cron(
                minute="0",
                hour="7",
                month="*",
                week_day="*",
                year="*",
            ),
            targets=[
                targets.SfnStateMachine(
                    self.state_machine,
                    input=events.RuleTargetInput.from_object({}),
                )
            ],
        )

        # =================================================================
        # Outputs
        # =================================================================
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
