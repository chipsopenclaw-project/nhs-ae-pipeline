#!/bin/bash
# NHS A&E Pipeline - AWS Infrastructure Setup
# Run with: bash infrastructure/setup.sh
# Requirements: AWS CLI configured with --profile chips

PROFILE="chips"
REGION="eu-west-2"
ACCOUNT_ID="502562061747"
RAW_BUCKET="chips-pipeline-raw"
PROCESSED_BUCKET="chips-pipeline-processed"
GLUE_ROLE="GlueETLRole"
CRAWLER_NAME="nhs-ae-crawler"
DATABASE_NAME="nhs_ae_db"

echo "=== Step 1: Create S3 Buckets ==="
aws s3 mb s3://$RAW_BUCKET --region $REGION --profile $PROFILE
aws s3 mb s3://$PROCESSED_BUCKET --region $REGION --profile $PROFILE

echo "=== Step 2: Upload Glue Script ==="
aws s3 cp scripts/glue_ae_pipeline.py s3://$RAW_BUCKET/scripts/glue_ae_pipeline.py \
  --region $REGION --profile $PROFILE

echo "=== Step 3: Upload Raw Data ==="
aws s3 cp data/raw/ae_synthetic_raw.csv s3://$RAW_BUCKET/ae/ae_synthetic_raw.csv \
  --region $REGION --profile $PROFILE

echo "=== Step 4: Create Glue Job ==="
# Note: IAM Role and Glue Job created via Console due to iam:PassRole restriction
echo "MANUAL STEP: Create Glue Job in Console"
echo "  - Name: nhs-ae-pipeline"
echo "  - Script: s3://$RAW_BUCKET/scripts/glue_ae_pipeline.py"
echo "  - IAM Role: $GLUE_ROLE"
echo "  - Glue version: 4.0, Worker: G.1X, Workers: 2"

echo "=== Step 5: Create Glue Database ==="
aws glue create-database \
  --database-input '{"Name": "'"$DATABASE_NAME"'"}' \
  --region $REGION --profile $PROFILE

echo "=== Step 6: Create Glue Crawler ==="
# Note: Requires iam:PassRole - create via Console if this fails
aws glue create-crawler \
  --name $CRAWLER_NAME \
  --role $GLUE_ROLE \
  --database-name $DATABASE_NAME \
  --targets '{"S3Targets": [{"Path": "s3://'"$PROCESSED_BUCKET"'/ae/"}]}' \
  --region $REGION --profile $PROFILE || \
  echo "MANUAL STEP: Create Crawler in Console pointing to s3://$PROCESSED_BUCKET/ae/"

echo "=== Step 7: Run Glue Job ==="
aws glue start-job-run \
  --job-name nhs-ae-pipeline \
  --region $REGION --profile $PROFILE

echo "=== Step 8: Run Crawler ==="
aws glue start-crawler \
  --name $CRAWLER_NAME \
  --region $REGION --profile $PROFILE

echo "=== Setup Complete ==="
echo "Query data in Athena:"
echo "  Database: $DATABASE_NAME"
echo "  Table: ae"
echo "  Results bucket: s3://$PROCESSED_BUCKET/athena-results/"
