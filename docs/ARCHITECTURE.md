# Architecture

## Overview

Petals is a data platform for market data, built on AWS with infrastructure-as-code (CDK). The architecture prioritizes cost efficiency and operational simplicity.

## System Diagram

```mermaid
flowchart TB
    subgraph External["External Services"]
        API[Massive API]
        CTGOV[ClinicalTrials.gov API]
        SM[Secrets Manager]
    end

    subgraph TickersPipeline["Tickers Pipeline (petals-tickers-pipeline)"]
        EB1[EventBridge<br/>Daily 6 AM UTC]
        SFN1[Step Functions]
        BATCH1[AWS Batch<br/>Fargate Spot]
    end

    subgraph TickerDetailsPipeline["Ticker Details Pipeline (petals-ticker-details-pipeline)"]
        EB3[EventBridge<br/>Daily 7 AM UTC]
        SFN3[Step Functions]
        BATCH3[AWS Batch<br/>Fargate Spot]
    end

    subgraph TrialsPipeline["Trials Pipeline (petals-trials-pipeline)"]
        EB2[EventBridge<br/>Daily 7 AM UTC]
        SFN2[Step Functions]
        BATCH2[AWS Batch<br/>Fargate Spot]
    end

    subgraph Shared["Shared Stack (petals-shared)"]
        ECR[ECR<br/>petals-pipelines]
        DDB[(DynamoDB<br/>Pipeline State)]
        subgraph S3Tables["S3 Tables (Iceberg)"]
            NS_REF[reference.*]
            NS_CLINICAL[clinical.*]
        end
    end

    subgraph Query["Query Layer"]
        ATH[Athena]
        USER[Analyst / dbt]
    end

    %% Tickers pipeline flow
    API --> BATCH1
    SM --> BATCH1
    EB1 --> SFN1
    SFN1 <-->|state| DDB
    SFN1 --> BATCH1
    ECR --> BATCH1
    BATCH1 -->|upsert| NS_REF

    %% Ticker details pipeline flow
    NS_REF -->|read US stock tickers| BATCH3
    API --> BATCH3
    SM --> BATCH3
    EB3 --> SFN3
    SFN3 <-->|state| DDB
    SFN3 --> BATCH3
    ECR --> BATCH3
    BATCH3 -->|upsert| NS_REF

    %% Trials pipeline flow
    CTGOV --> BATCH2
    EB2 --> SFN2
    SFN2 <-->|state| DDB
    SFN2 --> BATCH2
    ECR --> BATCH2
    BATCH2 -->|upsert| NS_CLINICAL

    %% Query flow
    USER --> ATH
    ATH --> S3Tables
```

## Data Flow

### Tickers Pipeline (daily, 6 AM UTC)
EventBridge triggers Step Functions → reads last run time from DynamoDB → submits Batch job → container fetches from Massive API (incremental) → upserts to S3 Tables `reference.tickers` → records new timestamp

### Ticker Details Pipeline (daily, 7 AM UTC)
EventBridge triggers Step Functions → reads last run time from DynamoDB → submits Batch job → reads US stock tickers from `reference.tickers` (`market='stocks'`, `locale='us'`) → fetches detailed info from Massive API (SIC codes, descriptions, etc.) → upserts to S3 Tables `reference.ticker_details` → records new timestamp

*Note: Initial backfill takes ~54 hours due to API rate limiting (5 calls/min). Incremental runs are much faster. Industry filtering (pharma/biotech via SIC codes) is done downstream.*

### Trials Pipeline (daily, 7 AM UTC)
EventBridge triggers Step Functions → reads last run time from DynamoDB → submits Batch job → container fetches COMPLETED studies from ClinicalTrials.gov (filtered to INDUSTRY sponsors) → upserts to S3 Tables `clinical.trials` → records new timestamp

### Query
Athena queries S3 Tables via federated catalog (`s3tablescatalog`)

## Namespaces

| Namespace | Table | Description |
|-----------|-------|-------------|
| `reference` | `tickers` | Stock/ETF ticker reference data from Massive API |
| `reference` | `ticker_details` | Enriched ticker details (SIC codes, descriptions) for US stock tickers |
| `clinical` | `trials` | Completed clinical trials from ClinicalTrials.gov (INDUSTRY sponsors) |

