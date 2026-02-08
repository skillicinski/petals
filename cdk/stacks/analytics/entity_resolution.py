"""
CDK stack for the entity resolution pipeline.

Part of the analytics layer (not ETL) - resolves entities across datasets.

Components:
- Batch compute environment (Fargate Spot for cost savings)
- Batch job queue and job definition
- Step Functions state machine for orchestration

On-demand only (no schedule) - trigger manually with: just trigger entity_resolution false

Uses sentence-transformer embeddings for semantic matching.
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
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class EntityResolutionStack(Stack):
    """Analytics stack for entity resolution workloads.

    Separate from ETL pipelines - depends on upstream data from pipelines layer.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Import shared resources
        table_bucket_arn = Fn.import_value("petals-table-bucket-arn")
        state_table_arn = Fn.import_value("petals-pipeline-state-table-arn")
        ecr_repo_arn = Fn.import_value("petals-ecr-repo-arn")
        artifacts_bucket_name = Fn.import_value("petals-artifacts-bucket-name")
        artifacts_bucket_arn = Fn.import_value("petals-artifacts-bucket-arn")

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
        # Batch Compute Environment (Spot for cost savings)
        # Pipeline is idempotent, Spot interruption is acceptable
        # =================================================================
        self.compute_env = batch.FargateComputeEnvironment(
            self,
            "EntityMatchComputeEnv",
            compute_environment_name="petals-entity-match-compute",
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            spot=True,  # Use Spot for cost savings
            maxv_cpus=4,
        )

        # =================================================================
        # Batch Job Queue
        # =================================================================
        self.job_queue = batch.JobQueue(
            self,
            "EntityMatchJobQueue",
            job_queue_name="petals-entity-match-queue",
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
            "EntityMatchJobRole",
            role_name="petals-entity-match-job-role",
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

        # S3 artifacts bucket access - for staging/recovery writes
        self.job_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:GetObject"],
                resources=[f"{artifacts_bucket_arn}/recovery/entity_resolution/*"],
            )
        )

        # =================================================================
        # Batch Job Definition
        # Embedding-based matching: ~21 seconds for full dataset
        # 4GB memory for sentence-transformer model (~500MB) + embeddings
        # =================================================================
        self.job_definition = batch.EcsJobDefinition(
            self,
            "EntityMatchJobDef",
            job_definition_name="petals-entity-match-job",
            container=batch.EcsFargateContainerDefinition(
                self,
                "EntityMatchContainer",
                image=ecs.ContainerImage.from_ecr_repository(self.ecr_repo, "latest"),
                cpu=1,
                memory=Size.mebibytes(4096),  # 4 GB for model + embeddings
                job_role=self.job_role,
                logging=ecs.LogDriver.aws_logs(
                    stream_prefix="entity-match-pipeline",
                    log_group=logs.LogGroup(
                        self,
                        "EntityMatchLogGroup",
                        log_group_name="/petals/pipelines/entity_resolution",
                        retention=logs.RetentionDays.TWO_WEEKS,
                        removal_policy=RemovalPolicy.DESTROY,
                    ),
                ),
                environment={
                    "TABLE_BUCKET_ARN": table_bucket_arn,
                    "ARTIFACTS_BUCKET": artifacts_bucket_name,
                    "AWS_DEFAULT_REGION": self.region,
                    "PIPELINE": "src.analytics.entity_resolution.main",
                    "SAVE_ICEBERG": "1",
                },
                assign_public_ip=True,
            ),
            timeout=Duration.minutes(30),  # Usually completes in <1 min
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
            key={"pipeline_id": tasks.DynamoAttributeValue.from_string("entity_resolution")},
            result_path="$.state",
        )

        # Task: Run Batch job with run_id from Step Functions execution
        run_batch = tasks.BatchSubmitJob(
            self,
            "RunEntityMatchPipeline",
            job_definition_arn=self.job_definition.job_definition_arn,
            job_queue_arn=self.job_queue.job_queue_arn,
            job_name="entity-match-pipeline",
            container_overrides=tasks.BatchContainerOverrides(
                environment={
                    # Use Step Functions execution ID as run_id for CloudWatch correlation
                    "RUN_ID": sfn.JsonPath.string_at("$$.Execution.Name"),
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
                "pipeline_id": tasks.DynamoAttributeValue.from_string("entity_resolution"),
                "last_run_time": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.State.EnteredTime")
                ),
                "last_status": tasks.DynamoAttributeValue.from_string("SUCCESS"),
            },
            result_path="$.update_result",
        )

        # Task: Record failure state
        record_failure = tasks.DynamoUpdateItem(
            self,
            "RecordFailure",
            table=self.state_table,
            key={
                "pipeline_id": tasks.DynamoAttributeValue.from_string("entity_resolution"),
            },
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

        # Build state machine definition (simpler than other pipelines - no incremental)
        definition = get_state.next(run_batch).next(update_state)

        self.state_machine = sfn.StateMachine(
            self,
            "EntityMatchPipelineStateMachine",
            state_machine_name="petals-entity-match-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(1),  # Usually completes in <1 min
            tracing_enabled=True,
        )

        # Grant state machine permissions
        self.state_table.grant_read_write_data(self.state_machine)

        # NOTE: No EventBridge schedule - entity matching is on-demand only
        # Trigger manually with: just trigger entity_resolution false

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
