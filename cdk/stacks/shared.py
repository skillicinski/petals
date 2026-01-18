from aws_cdk import (
    CfnOutput,
    RemovalPolicy,
    Stack,
)
from aws_cdk import (
    aws_athena as athena,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_s3 as s3,
)
from aws_cdk import (
    aws_s3tables as s3tables,
)
from constructs import Construct


class SharedStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 Table Bucket for Iceberg tables
        self.table_bucket = s3tables.CfnTableBucket(
            self,
            "TableBucket",
            table_bucket_name=f"petals-tables-{self.account}",
        )

        # IAM role for Lake Formation to access S3 Tables
        # This role is used by the S3 Tables integration with Lake Formation/Glue.
        # The integration itself is set up manually via Lake Formation console:
        #   1. Go to Lake Formation > Catalogs
        #   2. Click "Enable S3 Table integration"
        #   3. Select this role
        #   4. Check "Allow external engines to access data..."
        #
        # Key requirements for the role:
        # - Trust policy must allow sts:AssumeRole, sts:SetSourceIdentity, sts:TagSession
        # - Conditions should restrict to this account and Lake Formation service
        # - Role needs s3tables:* and s3:* permissions
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

        # Athena query results bucket
        self.athena_results_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            bucket_name=f"petals-athena-results-{self.account}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Athena workgroup with results location pre-configured
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

        # Outputs
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
