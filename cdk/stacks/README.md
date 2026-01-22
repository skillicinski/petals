# CDK Stacks

## SharedStack (`shared.py`)

Core infrastructure shared across all pipelines.

### Manual Setup: Lake Formation Grants

The Lake Formation API validates `CatalogId` as a 12-character AWS account ID, which prevents automation for S3 Tables federated catalogs. This applies to CloudFormation, AwsCustomResource, and direct boto3/CLI calls. The console uses an internal mechanism not exposed via public APIs.

**One-time setup to grant CLI user Athena access to S3 Tables:**

1. Go to [Lake Formation console](https://console.aws.amazon.com/lakeformation/) → **Data permissions**
2. Click **Grant**
3. **Principals**: IAM users and roles → `petals-cli`
4. **LF-Tags or catalog resources**: Select "Named Data Catalog resources"
5. **Catalogs**: `s3tablescatalog/petals-tables-<account-id>`
6. **Databases**: `reference`
7. **Tables**: All tables
8. **Table permissions**: Select, Describe
9. Click **Grant**

## TickersPipelineStack (`pipelines/tickers.py`)

Tickers data pipeline infrastructure:

- ECR repository for pipeline container
- Batch compute environment (Fargate Spot)
- Step Functions state machine for orchestration
- EventBridge rule for daily scheduling
