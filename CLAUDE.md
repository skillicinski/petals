# PETALS

This is personal portfolio project to display knowledge and skills in building a modern ETL platform and lightweight analytical data products that consume the platform's data.

Read more about the purpose and logic of the project in @README.md

## Useful Commands

Package and dependency management is handled with `uv`.

- `uv run` is preferred to, for example, make sure a local test run of a pipeline has all the necessary dependencies.

A justfile with recipes is used to collect and add concise commands that help with the development workflow of the project.

- `just health` shows the status of all developed pipelines and their target tables.
- `just logs {pipeline_name}` shows the CloudWatch logs of a given pipeline in the last 24 hours.
- `just test *args` runs all unit tests with optional arguments.
- `just deploy` runs tests, docker image build & push as well as the CDK deploy (skip_tests="false" ignores tests).
- `just trigger {pipeline_name}` starts a given pipeline.

## Data Retrieval
- An AWS Profile is required to be set for any local testing and scripting
```python
os.environ['AWS_PROFILE'] = os.environ.get('AWS_PROFILE', 'personal')
```

### AWS S3 Table Buckets

The preferred method for warehousing of data loaded through pipelines is as Parquet files in Iceberg tables. AWS has an Iceberg native storage method called Table Buckets, which has a separate API and structure than general purpose S3 buckets.

When we create tables within your Amazon S3 table bucket, we organize them into logical groupings called namespaces. Namespaces aren't resources, they are constructs that help organize and manage our tables in a scalable manner. For example, all the tables that contain financial market data could be grouped in the namespace `market`.

The AWS CLI uses `s3tables` to communicate with Table Bucket APIs, for example to list the first table bucket in a region:
```bash
aws s3tables list-table-buckets --profile personal --region us-east-1 --output json --query 'tableBuckets[0].arn'
```

Or, to get the status of an automated table maintenance process:
```bash
aws s3tables get-table-maintenance-job-status \
   --table-bucket-arn="arn:aws:s3tables:arn:aws::111122223333:bucket/amzn-s3-demo-bucket1" \
   --namespace="mynamespace" \
   --name="testtable" 
```

- Table namespaces are referred to as databases in various AWS services and query engines. For example, a namespace maps to a database in AWS Athena, AWS Glue Data Catalog and AWS Lake Formation.
- S3 Tables uses its own managed Iceberg catalog and REST API, which can be interacted with using the Pyiceberg API
```python
# Connect to S3 Tables catalog
catalog = load_catalog(
    name='s3tables',
    type='rest',
    uri=f'https://s3tables.{region}.amazonaws.com/iceberg',
    warehouse=table_bucket_arn,
    **{
        'rest.sigv4-enabled': 'true',
        'rest.signing-region': region,
        'rest.signing-name': 's3tables',
    }
)
```
- Pyiceberg supports loading S3 Table data and into a polars DataFrame or LazyFrame
```python
# Use PyIceberg's scan API to convert to Polars
df = table.scan().to_polars()
```

## Linting and formatting

We use `ruff` to follow PEP8 formatting.

## Testing

On all changes to source Python code that is deployed, we want to run comprehensive unit tests and integration tests. If these are not present, suggest adding them. For our foundational test pattern we use `pytest` with `localstack` and `moto`.

## Debugging

Typical debugging tasks start by checking the CloudWatch logs for the relevant Log Group and the most recent Log Stream in it.

## Commits & PRs

- Use fix, feature, chore, refactor prefixes for naming commit and PRs.
- Include my Github user as a reviewer on any PR.
- Do not include any mention of Claude, Cursor or any other AI tools as authors or co-authors in any commits or PRs.
- Always remove any mention of Cursor, Windsurf, Claude Code or other agents from commits and PRs, eg. `--trailer "Co-authored-by: Cursor <cursoragent@cursor.com>"`
- Do not commit the DEVLOG.md