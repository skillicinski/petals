"""
CDK stack for the ticker prices (OHLC) pipeline.

Fetches daily OHLC price data for US stocks using yfinance.

Components:
- Batch compute environment (Fargate Spot for cost optimization)
- Batch job queue and job definition
- Step Functions state machine for orchestration
- EventBridge rule for daily scheduling (after market close)

Note: No secrets needed - yfinance is free and doesn't require API keys.
Much faster than Polygon free tier (~10-15 min vs days for full US market).
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
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class TickerPricesPipelineStack(Stack):
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
        # Batch Compute Environment (Fargate Spot)
        # =================================================================
        self.compute_env = batch.FargateComputeEnvironment(
            self,
            "TickerPricesComputeEnv",
            compute_environment_name="petals-ticker-prices-compute",
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            spot=True,  # Use Spot for cost savings (pipeline is ~10-15 min)
            maxv_cpus=4,
        )

        # =================================================================
        # Batch Job Queue
        # =================================================================
        self.job_queue = batch.JobQueue(
            self,
            "TickerPricesJobQueue",
            job_queue_name="petals-ticker-prices-queue",
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
            "TickerPricesJobRole",
            role_name="petals-ticker-prices-job-role",
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

        # =================================================================
        # Batch Job Definition
        # =================================================================
        self.job_definition = batch.EcsJobDefinition(
            self,
            "TickerPricesJobDef",
            job_definition_name="petals-ticker-prices-job",
            container=batch.EcsFargateContainerDefinition(
                self,
                "TickerPricesContainer",
                image=ecs.ContainerImage.from_ecr_repository(self.ecr_repo, "latest"),
                cpu=0.5,
                memory=Size.mebibytes(2048),  # 2 GB for processing price data
                job_role=self.job_role,
                logging=ecs.LogDriver.aws_logs(
                    stream_prefix="ticker-prices-pipeline",
                    log_group=logs.LogGroup(
                        self,
                        "TickerPricesLogGroup",
                        log_group_name="/petals/pipelines/ticker_prices",
                        retention=logs.RetentionDays.TWO_WEEKS,
                        removal_policy=RemovalPolicy.DESTROY,
                    ),
                ),
                environment={
                    "TABLE_BUCKET_ARN": table_bucket_arn,
                    "AWS_DEFAULT_REGION": self.region,
                    "PIPELINE": "src.pipelines.ticker_prices.main",
                },
                # No secrets needed - yfinance is free
                assign_public_ip=True,  # Required to reach ECR/internet without NAT
            ),
            timeout=Duration.minutes(30),  # ~10-15 min for full US market
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
            key={"pipeline_id": tasks.DynamoAttributeValue.from_string("ticker_prices")},
            result_path="$.state",
        )

        # Task: Run Batch job (FORCE_FULL handled in code if needed)
        run_batch = tasks.BatchSubmitJob(
            self,
            "RunTickerPricesPipeline",
            job_definition_arn=self.job_definition.job_definition_arn,
            job_queue_arn=self.job_queue.job_queue_arn,
            job_name="ticker-prices-pipeline",
            result_path="$.batch_result",
        )

        # Task: Update state with new run time (success)
        update_state = tasks.DynamoUpdateItem(
            self,
            "UpdatePipelineState",
            table=self.state_table,
            key={"pipeline_id": tasks.DynamoAttributeValue.from_string("ticker_prices")},
            update_expression=(
                "SET last_run_time = :last_run_time, last_status = :last_status REMOVE #err, cause"
            ),
            expression_attribute_names={"#err": "error"},
            expression_attribute_values={
                ":last_run_time": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.State.EnteredTime")
                ),
                ":last_status": tasks.DynamoAttributeValue.from_string("SUCCESS"),
            },
            result_path=sfn.JsonPath.DISCARD,
        )

        # Task: Record failure
        record_failure = tasks.DynamoUpdateItem(
            self,
            "RecordFailure",
            table=self.state_table,
            key={"pipeline_id": tasks.DynamoAttributeValue.from_string("ticker_prices")},
            update_expression="SET last_status = :status, #err = :error, cause = :cause",
            expression_attribute_names={"#err": "error"},
            expression_attribute_values={
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
        record_failure.next(fail_state)

        # Add error handling to Batch job
        run_batch.add_catch(
            record_failure,
            errors=["States.ALL"],
            result_path="$.error",
        )

        # Chain tasks together
        definition = get_state.next(run_batch).next(update_state)

        # State machine
        self.state_machine = sfn.StateMachine(
            self,
            "TickerPricesStateMachine",
            state_machine_name="petals-ticker-prices-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.minutes(45),  # Buffer above job timeout
            tracing_enabled=True,
        )

        # Grant permissions
        self.state_table.grant_read_write_data(self.state_machine)

        # =================================================================
        # EventBridge Schedule (daily in the morning UTC)
        # US market closes at 4 PM ET (9 PM UTC)
        # Run at 7 AM UTC (8 AM CET Stockholm) - previous day's data is complete
        # Pipeline defaults to fetching yesterday's data, so this is optimal
        # =================================================================
        self.schedule_rule = events.Rule(
            self,
            "TickerPricesSchedule",
            rule_name="petals-ticker-prices-daily",
            schedule=events.Schedule.cron(
                minute="0",
                hour="7",  # 7 AM UTC = 8 AM CET (Stockholm winter)
                month="*",
                week_day="MON-FRI",  # Only run on trading days
                year="*",
            ),
        )

        self.schedule_rule.add_target(
            targets.SfnStateMachine(
                self.state_machine,
                input=events.RuleTargetInput.from_object({"force_full": False}),
            )
        )

        # =================================================================
        # Outputs
        # =================================================================
        CfnOutput(
            self,
            "StateMachineArn",
            value=self.state_machine.state_machine_arn,
            description="Ticker Prices Pipeline State Machine ARN",
        )

        CfnOutput(
            self,
            "JobDefinitionArn",
            value=self.job_definition.job_definition_arn,
            description="Ticker Prices Batch Job Definition ARN",
        )
