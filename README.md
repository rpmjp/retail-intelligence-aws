# Retail Intelligence AWS Data Pipeline

End-to-end AWS data engineering pipeline that ingests retail transaction data, processes it through a data lake architecture, and exposes it for analytics via Athena and Redshift.

## Architecture

*Diagram coming soon - see `architecture/`.*

## AWS Services

- **S3** - data lake (raw, curated zones)
- **Lambda** - event-driven transforms
- **Glue** - PySpark ETL jobs + Data Catalog
- **Step Functions** - pipeline orchestration
- **Athena** - SQL over S3
- **Redshift Serverless** - analytics warehouse
- **Kinesis** - streaming ingestion
- **CloudFormation** - infrastructure as code

## Project Status

In active development.

## Repository Structure

```
infrastructure/   CloudFormation templates
ingestion/        Batch + streaming data producers
lambdas/          Lambda function code
glue_jobs/        PySpark ETL scripts
step_functions/   State machine definitions
queries/          Athena + Redshift SQL
screenshots/      AWS Console proofs of execution
tests/            Unit tests with moto-mocked AWS
```