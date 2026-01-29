from aws_cdk import (
    CfnOutput,
    RemovalPolicy,
    Stack,
)
from aws_cdk import aws_athena as athena
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3tables as s3tables
from constructs import Construct


class SharedStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # =================================================================
        # S3 Tables / Lake Formation
        # =================================================================
        # Table bucket for Iceberg tables
        self.table_bucket = s3tables.CfnTableBucket(
            self,
            "TableBucket",
            table_bucket_name=f"petals-tables-{self.account}",
        )

        # Role for S3 Tables integration with Lake Formation/Glue.
        # The integration itself is set up manually via Lake Formation console:
        #   1. Go to Lake Formation > Catalogs
        #   2. Click "Enable S3 Table integration"
        #   3. Select this role
        #   4. Check "Allow external engines to access data..."
        self.lakeformation_role = iam.Role(
            self,
            "LakeFormationS3TablesRole",
            role_name="PetalsLakeFormationS3TablesRole",
            assumed_by=iam.ServicePrincipal(
                "lakeformation.amazonaws.com",
                conditions={
                    "StringEquals": {"aws:SourceAccount": self.account},
                    "ArnEquals": {
                        "aws:SourceArn": f"arn:aws:lakeformation:{self.region}:{self.account}:*"
                    },
                },
            ),
            inline_policies={
                "S3TablesFullAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["s3tables:*", "s3:*"],
                            resources=["*"],
                        )
                    ]
                )
            },
        )

        # Add sts:SetSourceIdentity and sts:TagSession to trust policy
        # (CDK's assumed_by doesn't support multiple actions, so we add via escape hatch)
        cfn_role = self.lakeformation_role.node.default_child
        cfn_role.add_property_override(
            "AssumeRolePolicyDocument.Statement.0.Action",
            ["sts:AssumeRole", "sts:SetSourceIdentity", "sts:TagSession"],
        )

        # =================================================================
        # Athena (query layer)
        # =================================================================
        self.athena_results_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            bucket_name=f"petals-athena-results-{self.account}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.athena_workgroup = athena.CfnWorkGroup(
            self,
            "AthenaWorkgroup",
            name="petals",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{self.athena_results_bucket.bucket_name}/",
                ),
                enforce_work_group_configuration=True,
            ),
        )

        # =================================================================
        # CLI User (for local development / Athena queries)
        # =================================================================
        # Primary IAM user for CLI access. Created via CDK for full lifecycle
        # management. To import an existing user into CDK management:
        #   cdk import petals-shared AWS::IAM::User CliUser
        # Then provide the username "petals-cli" when prompted.
        #
        # Access keys are managed separately (console or CLI) and are not
        # affected by CDK deployments.
        self.cli_user = iam.User(
            self,
            "CliUser",
            user_name="petals-cli",
        )

        # IAM policy for Athena / S3 Tables access
        cli_athena_policy = iam.Policy(
            self,
            "CliUserAthenaPolicy",
            policy_name="petals-cli-athena-access",
            statements=[
                # Athena workgroup access
                iam.PolicyStatement(
                    sid="AthenaWorkgroup",
                    actions=[
                        "athena:StartQueryExecution",
                        "athena:StopQueryExecution",
                        "athena:GetQueryExecution",
                        "athena:GetQueryResults",
                        "athena:GetWorkGroup",
                        "athena:ListQueryExecutions",
                        "athena:BatchGetQueryExecution",
                    ],
                    resources=[
                        f"arn:aws:athena:{self.region}:{self.account}:workgroup/petals",
                    ],
                ),
                # S3 results bucket access
                iam.PolicyStatement(
                    sid="AthenaResultsBucket",
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                        "s3:AbortMultipartUpload",
                    ],
                    resources=[
                        self.athena_results_bucket.bucket_arn,
                        f"{self.athena_results_bucket.bucket_arn}/*",
                    ],
                ),
                # Glue catalog access (for Athena metadata queries)
                iam.PolicyStatement(
                    sid="GlueCatalog",
                    actions=[
                        "glue:GetDatabase",
                        "glue:GetDatabases",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:GetPartition",
                        "glue:GetPartitions",
                        "glue:BatchGetPartition",
                    ],
                    resources=[
                        f"arn:aws:glue:{self.region}:{self.account}:catalog",
                        f"arn:aws:glue:{self.region}:{self.account}:database/*",
                        f"arn:aws:glue:{self.region}:{self.account}:table/*/*",
                    ],
                ),
                # Lake Formation access (required for S3 Tables federated catalog)
                iam.PolicyStatement(
                    sid="LakeFormation",
                    actions=[
                        "lakeformation:GetDataAccess",
                    ],
                    resources=["*"],
                ),
                # S3 Tables read access (for direct table queries)
                iam.PolicyStatement(
                    sid="S3TablesRead",
                    actions=[
                        "s3tables:GetTable",
                        "s3tables:GetTableBucket",
                        "s3tables:GetTableData",
                        "s3tables:ListTables",
                        "s3tables:ListTableBuckets",
                        "s3tables:GetNamespace",
                        "s3tables:ListNamespaces",
                    ],
                    resources=[
                        self.table_bucket.attr_table_bucket_arn,
                        f"{self.table_bucket.attr_table_bucket_arn}/*",
                    ],
                ),
            ],
        )
        cli_athena_policy.attach_to_user(self.cli_user)

        # Lake Formation grants for S3 Tables federated catalog must be
        # configured manually via console. See README.md in this directory.

        # =================================================================
        # Shared ECR Repository (all pipelines)
        # =================================================================
        # Single image containing all pipeline code. Each pipeline sets
        # the PIPELINE env var to select which module to run.
        self.ecr_repo = ecr.Repository(
            self,
            "PipelinesRepo",
            repository_name="petals-pipelines",
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                ecr.LifecycleRule(
                    max_image_count=10,
                    description="Keep only 10 images",
                )
            ],
        )

        # =================================================================
        # Pipeline State (DynamoDB)
        # =================================================================
        # Stores last run timestamps, status, etc. for all pipelines.
        # Single table with pipeline_id partition key for cost efficiency.
        self.pipeline_state_table = dynamodb.Table(
            self,
            "PipelineState",
            table_name="petals-pipeline-state",
            partition_key=dynamodb.Attribute(
                name="pipeline_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # =================================================================
        # Outputs
        # =================================================================
        CfnOutput(
            self,
            "TableBucketName",
            value=self.table_bucket.table_bucket_name,
            export_name="petals-table-bucket-name",
            description="S3 Table Bucket name",
        )

        CfnOutput(
            self,
            "TableBucketArn",
            value=self.table_bucket.attr_table_bucket_arn,
            export_name="petals-table-bucket-arn",
            description="S3 Table Bucket ARN",
        )

        CfnOutput(
            self,
            "PipelineStateTableName",
            value=self.pipeline_state_table.table_name,
            export_name="petals-pipeline-state-table-name",
            description="DynamoDB table for pipeline state",
        )

        CfnOutput(
            self,
            "PipelineStateTableArn",
            value=self.pipeline_state_table.table_arn,
            export_name="petals-pipeline-state-table-arn",
            description="DynamoDB table ARN for pipeline state",
        )

        CfnOutput(
            self,
            "ECRRepositoryArn",
            value=self.ecr_repo.repository_arn,
            export_name="petals-ecr-repo-arn",
            description="Shared ECR repository ARN for all pipelines",
        )

        CfnOutput(
            self,
            "ECRRepositoryUri",
            value=self.ecr_repo.repository_uri,
            export_name="petals-ecr-repo-uri",
            description="Shared ECR repository URI for docker push",
        )
