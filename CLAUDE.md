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

## Linting and formatting

We use `ruff` to follow PEP8 formatting.

## Debugging

Typical debugging tasks start by checking the CloudWatch logs for the relevant Log Group and the most recent Log Stream in it.

## Commits and PRs

- Use fix, feature, chore, refactor prefixes for commits.
- Include my Github user as a reviewer on any PR.
- Do not include any mention of Claude, Cursor or any other AI tools as authors or co-authors in any commits or PRs.