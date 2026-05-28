#!/usr/bin/env bash
# Creates Glue database, ETL job, and crawler for the retail pipeline.
# Prerequisites: S3 buckets and Glue IAM role already deployed via CloudFormation.
set -euo pipefail

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
PROJECT="retail-intelligence"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${PROJECT}-glue-role"
RAW="s3://${PROJECT}-raw-${ACCOUNT_ID}/online_retail/online_retail_II.csv"
CURATED="s3://${PROJECT}-curated-${ACCOUNT_ID}/online_retail/"
SCRIPT="s3://${PROJECT}-curated-${ACCOUNT_ID}/scripts/clean_retail_data.py"

echo "Uploading Glue script..."
aws s3 cp glue_jobs/clean_retail_data.py "$SCRIPT"

echo "Creating Glue database..."
aws glue create-database --database-input '{"Name":"retail_intelligence"}' || true

echo "Creating Glue job..."
aws glue create-job \
  --name retail-clean-job \
  --role "$ROLE_ARN" \
  --command "{\"Name\":\"glueetl\",\"ScriptLocation\":\"${SCRIPT}\",\"PythonVersion\":\"3\"}" \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X \
  --default-arguments "{\"--RAW_PATH\":\"${RAW}\",\"--CURATED_PATH\":\"${CURATED}\"}" || true

echo "Creating Glue crawler..."
aws glue create-crawler \
  --name retail-curated-crawler \
  --role "$ROLE_ARN" \
  --database-name retail_intelligence \
  --targets "{\"S3Targets\":[{\"Path\":\"${CURATED}\"}]}" || true

echo "Done. Run the job:  aws glue start-job-run --job-name retail-clean-job"
echo "Then the crawler:   aws glue start-crawler --name retail-curated-crawler"