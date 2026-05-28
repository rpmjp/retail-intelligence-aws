# Retail Intelligence AWS Data Pipeline

An end-to-end, infrastructure-as-code data engineering pipeline on AWS that ingests retail transaction data, transforms it through a partitioned data lake, catalogs it, and exposes it for SQL analytics. Built to demonstrate production data engineering patterns: serverless ETL, workflow orchestration, the Glue Data Catalog, and reproducible infrastructure.

The dataset is the [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii) set: roughly 1.07 million real e-commerce transactions across 2009-2011, complete with the nulls, cancellations, and negative quantities found in real operational data.

---

## Architecture

```
Online Retail II (Excel, ~1.07M rows)
        |
        v
  [ Python batch ingestion ]
        |
        v
  S3 Raw Zone  (CSV, ~95 MB)
        |
        v
  Step Functions orchestration
        |
        +--> AWS Glue PySpark job  (clean, enrich, repartition)
        |          |
        |          v
        |    S3 Curated Zone  (Parquet, partitioned by year / month)
        |          |
        +--> Glue Crawler --> Glue Data Catalog (table: online_retail)
                   |
                   v
            Amazon Athena  (serverless SQL over S3)
```

Every AWS resource in this project is defined in CloudFormation and created from the CLI, so the entire stack is reproducible from source.

---

## AWS Services Used

| Service | Role in the pipeline |
|---|---|
| **S3** | Two-zone data lake: raw (landing) and curated (processed Parquet) |
| **AWS Glue (PySpark)** | Serverless Spark ETL: cleaning, enrichment, repartitioning |
| **Glue Crawler + Data Catalog** | Schema inference and table registration over the curated zone |
| **Step Functions** | Orchestrates the ETL job and crawler as a single state machine with error handling |
| **Amazon Athena** | Serverless SQL analytics directly against partitioned Parquet |
| **CloudFormation** | Infrastructure as code for all S3 buckets and IAM roles |
| **IAM** | Least-privilege service roles for Glue and Step Functions |

---

## Repository Structure

```
retail-intelligence-aws/
├── infrastructure/
│   └── cloudformation/
│       ├── 01-s3-buckets.yaml          # Raw + curated data lake buckets
│       ├── 02-glue-role.yaml           # Glue service IAM role
│       └── 03-stepfunctions-role.yaml  # Step Functions orchestration role
├── ingestion/
│   └── batch/
│       └── upload_raw_to_s3.py         # Load Excel, combine sheets, push to raw zone
├── glue_jobs/
│   └── clean_retail_data.py            # PySpark ETL: clean + partition to Parquet
├── step_functions/
│   └── pipeline_definition.json        # State machine: ETL -> crawler, with catch states
├── queries/
│   └── athena/
│       └── 01_monthly_revenue.sql      # Example analytics query
├── scripts/
│   └── setup_glue_resources.sh         # Reproducibly create Glue DB, job, crawler
├── screenshots/                        # Console proof of execution
├── tests/
│   └── test_placeholder.py
├── .github/workflows/ci.yml            # Lint (ruff) + format (black) + test (pytest)
├── pyproject.toml
└── requirements.txt
```

---

## Pipeline Walkthrough

### 1. Ingestion: raw zone

`ingestion/batch/upload_raw_to_s3.py` reads both sheets of the Online Retail II Excel file, concatenates them into a single ~1.07M-row frame, writes a combined CSV, and uploads it to the raw S3 zone. The raw zone keeps source data immutable, which is a core data-lake principle: never transform in place; always derive forward.

### 2. Transformation: Glue PySpark

`glue_jobs/clean_retail_data.py` runs on serverless Spark (Glue 4.0, 2× G.1X workers) and performs:

- Removes rows with null `Customer ID` (un-attributable transactions)
- Filters out cancelled invoices (invoice numbers beginning with `C`)
- Filters out non-positive quantity and price (returns, data errors)
- Derives a `Revenue` column (`Quantity × UnitPrice`)
- Parses `InvoiceDate` to a timestamp and extracts `year` / `month`
- Writes Snappy-compressed Parquet partitioned by `year` and `month`

Partitioning by date means queries that filter on a time range scan only the relevant partitions instead of the whole dataset, which is the core mechanism behind the efficiency gains shown below.

### 3. Cataloging: Glue Crawler

The crawler scans the curated zone, infers the schema (11 columns, with `year` and `month` recognized as partition keys), and registers the `online_retail` table in the `retail_intelligence` Glue database. Athena then queries that table without any manual schema definition.

### 4. Orchestration: Step Functions

`step_functions/pipeline_definition.json` defines a state machine that runs the Glue job synchronously, then triggers the crawler, with `Catch` states routing any failure to a dedicated `PipelineFailed` state. This turns a sequence of manual CLI calls into a single auditable, re-runnable workflow with built-in error handling.

### 5. Analytics: Athena

Athena queries the catalog table directly. The example query aggregates monthly revenue, distinct orders, and distinct customers across the full date range.

---

## Results

The monthly revenue query surfaces a clear seasonal pattern: revenue climbs toward Q4 each year, peaking in October and November (holiday season) above £1M per month (roughly $1.34M at current exchange rates), then drops sharply after the December cutoff in the data.

**Query efficiency:** the same monthly-revenue aggregation scanned only **1.70 MB** against the partitioned Parquet curated zone, versus the full ~95 MB raw CSV that a naive scan would read. This large reduction in data scanned is the combined payoff of columnar Parquet, Snappy compression, and date partitioning. In Athena, which bills per terabyte scanned, less data scanned translates directly into lower query cost.

> Currency note: the source retailer is UK-based, so revenue is in pounds sterling (GBP). USD figures use an approximate rate of £1 = $1.34 and will drift with the market.

---

## Proof of Execution

All screenshots are in [`screenshots/`](screenshots/).

| Stage | Screenshot |
|---|---|
| S3 data lake (raw + curated buckets) | `screenshots/s3/buckets.png` |
| Partitioned Parquet (`year=` / `month=`) | `screenshots/s3/partitions.png` |
| Glue ETL job, Succeeded | `screenshots/glue/job_run.png` |
| Glue Data Catalog table schema | `screenshots/glue/catalog_table.png` |
| Athena query results (1.70 MB scanned) | `screenshots/athena/monthly_revenue.png` |
| Step Functions execution graph | `screenshots/step_functions/execution_graph.png` |

---

## Reproducing This Pipeline

The full stack is built from source. With the AWS CLI configured:

```bash
# 1. Deploy infrastructure (S3 buckets + IAM roles)
aws cloudformation deploy --template-file infrastructure/cloudformation/01-s3-buckets.yaml \
  --stack-name retail-s3-buckets --parameter-overrides ProjectName=retail-intelligence
aws cloudformation deploy --template-file infrastructure/cloudformation/02-glue-role.yaml \
  --stack-name retail-glue-role --parameter-overrides ProjectName=retail-intelligence \
  --capabilities CAPABILITY_NAMED_IAM
aws cloudformation deploy --template-file infrastructure/cloudformation/03-stepfunctions-role.yaml \
  --stack-name retail-stepfunctions-role --parameter-overrides ProjectName=retail-intelligence \
  --capabilities CAPABILITY_NAMED_IAM

# 2. Ingest the dataset to the raw zone
python ingestion/batch/upload_raw_to_s3.py

# 3. Create Glue resources (database, job, crawler)
bash scripts/setup_glue_resources.sh

# 4. Run the orchestrated pipeline
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:<ACCOUNT_ID>:stateMachine:retail-pipeline
```

> **Note on infrastructure lifecycle:** This project is designed to be **stood up on demand, validated, captured, and torn down** rather than left running. Glue, Crawlers, and Athena are billed per use, so running the pipeline costs only cents per execution. The screenshots in this repo are the proof of successful runs; the infrastructure itself is fully reproducible from the CloudFormation templates and scripts above whenever a live demo is needed.

---

## Design Decisions and Lessons Learned

**Parquet over CSV in the curated zone.** Columnar Parquet with Snappy compression is the standard analytics-zone format. It enables predicate pushdown and column pruning, so Athena reads only the columns and partitions a query touches. The 1.70 MB vs ~95 MB scan figure is this decision made measurable.

**Partition by year/month, not finer.** Date partitioning matches the dominant query pattern (time-range analytics) without over-partitioning. Partitioning on something high-cardinality like `CustomerID` would create millions of tiny files and *degrade* performance, the classic small-files problem. Month granularity keeps partition counts and file sizes healthy.

**Step Functions instead of chained Lambda or cron.** A state machine gives a visual execution history, native synchronous waiting on the Glue job (`.sync`), and declarative error handling via `Catch`. The `PipelineFailed` state never fires on a healthy run, but its presence means the pipeline is built for failure, not just the happy path, which is what separates a production pipeline from a script.

**Separate IAM roles per service, least privilege.** The Glue role can read raw / write curated and nothing else; the Step Functions role can start and monitor Glue resources and nothing else. Scoping roles to exactly what each service needs limits blast radius if a credential is ever compromised.

**String columns over enums; soft, forward-only transformation.** The raw zone is never mutated. Every transformation writes a new derived artifact in the curated zone, so any processing bug can be fixed and re-run from immutable source without data loss.

**CI from commit one.** A GitHub Actions workflow lints (ruff), checks formatting (black), and runs tests (pytest) on every push. Code quality is enforced mechanically rather than by memory, which is the same discipline applied to the infrastructure itself.

---

## Tech Stack

**Languages:** Python 3.12, PySpark, SQL, Bash
**AWS:** S3, Glue, Athena, Step Functions, CloudFormation, IAM
**Data formats:** Parquet (Snappy), CSV
**Tooling:** boto3, pandas, ruff, black, pytest, GitHub Actions

---

## Roadmap

- **Kinesis streaming ingestion**: simulate a live order stream into the lake alongside the batch path
- **Redshift Serverless**: load the curated zone into a warehouse for BI-style workloads
- **QuickSight dashboard**: native AWS visualization over the Athena tables
