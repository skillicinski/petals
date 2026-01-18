# Design Document

## Principles

**Data outlives pipelines.** Extraction logic will be refactored, replaced, or deprecated while the data remains valuable. Infrastructure lifecycle should not dictate data lifecycle.

**Pipelines are standalone.** Each pipeline is deployed independently without impacting others. Isolated blast radius, simple reasoning.

**Pipelines are idempotent.** Every process that involves data should produce predictable outputs. Change detection is key to produce efficient writes of new data without altering closed historical information.