# Architecture

## Overview

Petals is a data platform for market data, built on AWS with infrastructure-as-code (CDK). The architecture prioritizes cost efficiency and operational simplicity.

## System Diagram

```mermaid
flowchart TB
    subgraph External["External Services"]
        API[Massive API]
        SM[Secrets Manager]
    end

    subgraph Orchestration["Tickers Pipeline (petals-tickers-pipeline)"]
        EB[EventBridge<br/>Daily 6 AM UTC]
        SFN[Step Functions]
        BATCH[AWS Batch<br/>Fargate Spot]
        ECR[ECR]
    end

    subgraph Storage["Shared Stack (petals-shared)"]
        direction LR
        DDB[(DynamoDB<br/>Pipeline State)]
        S3T[(S3 Tables<br/>Iceberg)]
    end

    subgraph Query["Query Layer"]
        ATH[Athena]
        USER[Analyst / dbt]
    end

    %% Main pipeline flow (top to bottom)
    API --> BATCH
    SM --> BATCH
    EB --> SFN
    SFN <-->|state| DDB
    SFN --> BATCH
    ECR --> BATCH
    BATCH -->|upsert| S3T

    %% Query flow (parallel, side access to storage)
    USER --> ATH
    ATH --> S3T
```

## Data Flow

**Pipeline (daily):**
EventBridge triggers Step Functions → reads last run time from DynamoDB → submits Batch job → container fetches from Massive API (incremental) → upserts to S3 Tables → records new timestamp

**Query:**
Athena queries S3 Tables via federated catalog (`s3tablescatalog`)
