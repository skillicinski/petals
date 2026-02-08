"""Analytics workloads for deriving insights from data.

Separate from ETL pipelines - focuses on:
- Entity resolution (matching entities across datasets)
- Feature engineering
- Aggregations and derived datasets
- ML model training and inference

Design principle: Analytics depends on pipelines (upstream data),
but pipelines should never depend on analytics (downstream insights).
"""
