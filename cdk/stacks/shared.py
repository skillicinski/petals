from aws_cdk import (
    CfnOutput,
    Stack,
    aws_s3tables as s3tables,
)
from constructs import Construct


class SharedStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.table_bucket = s3tables.CfnTableBucket(
            self,
            "TableBucket",
            table_bucket_name=f"petals-tables-{self.account}",
        )

        # Outputs for use by pipelines
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
