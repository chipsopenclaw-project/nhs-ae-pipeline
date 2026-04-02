import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime

# -----------------------------------------------
# Logging Setup
# -----------------------------------------------
MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger("nhs-ae-pipeline")
logger.setLevel(logging.INFO)

# -----------------------------------------------
# Init Glue
# -----------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_PATH = "s3://chips-pipeline-raw/ae/ae_synthetic_raw.csv"
PROCESSED_PATH = "s3://chips-pipeline-processed/ae/"
MIN_ROW_COUNT = 1000
MAX_REMOVAL_PCT = 0.10

# -----------------------------------------------
# EXTRACT
# -----------------------------------------------
try:
    logger.info(f"Reading raw data from: {RAW_PATH}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(RAW_PATH)
    raw_count = df.count()
    logger.info(f"Raw row count: {raw_count}")

    if raw_count < MIN_ROW_COUNT:
        raise ValueError(f"FAILED: Raw data too small ({raw_count} rows). Min required: {MIN_ROW_COUNT}")

except Exception as e:
    logger.error(f"EXTRACT failed: {str(e)}")
    raise

# -----------------------------------------------
# TRANSFORM
# -----------------------------------------------
try:
    logger.info("Starting data cleaning...")

    # 1. Drop nulls on critical fields
    df_clean = df.dropna(subset=["attendance_id", "arrival_datetime", "ae_department_type"])

    # 2. Remove invalid wait times
    df_clean = df_clean.filter(F.col("wait_minutes") >= 0)

    # 3. Standardise nulls
    df_clean = df_clean.fillna({"gender": "Unknown", "age_band": "Unknown"})

    # 4. Trim whitespace on string columns
    string_cols = ["ae_department_type", "arrival_mode", "age_band", "gender", "icd10_chapter", "provider_code"]
    for col in string_cols:
        df_clean = df_clean.withColumn(col, F.trim(F.col(col)))

    # 5. Derived columns
    df_clean = df_clean \
        .withColumn("four_hour_breach", F.when(F.col("wait_minutes") > 240, 1).otherwise(0)) \
        .withColumn("arrival_hour", F.hour(F.col("arrival_datetime"))) \
        .withColumn("arrival_month", F.month(F.col("arrival_datetime"))) \
        .withColumn("processed_timestamp", F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))

    logger.info("Data cleaning complete")

except Exception as e:
    logger.error(f"TRANSFORM failed: {str(e)}")
    raise

# -----------------------------------------------
# DATA QUALITY CHECKS
# -----------------------------------------------
try:
    logger.info("Running data quality checks...")

    cleaned_count = df_clean.count()
    removed_count = raw_count - cleaned_count
    removed_pct = removed_count / raw_count
    breach_count = df_clean.filter(F.col("four_hour_breach") == 1).count()
    breach_pct = round((breach_count / cleaned_count) * 100, 2)
    null_gender = df_clean.filter(F.col("gender").isNull()).count()
    null_age = df_clean.filter(F.col("age_band").isNull()).count()

    logger.info(f"Cleaned row count:     {cleaned_count}")
    logger.info(f"Rows removed:          {removed_count} ({round(removed_pct*100,1)}%)")
    logger.info(f"4-hour breach rate:    {breach_pct}%")
    logger.info(f"Null gender remaining: {null_gender}")
    logger.info(f"Null age remaining:    {null_age}")

    # Quality gates
    assert cleaned_count >= MIN_ROW_COUNT, \
        f"FAILED: Cleaned row count too low ({cleaned_count})"
    assert removed_pct <= MAX_REMOVAL_PCT, \
        f"FAILED: Too many rows removed ({round(removed_pct*100,1)}% > {MAX_REMOVAL_PCT*100}%)"
    assert null_gender == 0, \
        f"FAILED: Null gender values remaining ({null_gender})"
    assert null_age == 0, \
        f"FAILED: Null age_band values remaining ({null_age})"

    logger.info("All data quality checks PASSED")

except AssertionError as e:
    logger.error(f"DATA QUALITY CHECK failed: {str(e)}")
    raise
except Exception as e:
    logger.error(f"DATA QUALITY CHECK error: {str(e)}")
    raise

# -----------------------------------------------
# LOAD
# -----------------------------------------------
try:
    logger.info(f"Writing processed data to: {PROCESSED_PATH}")
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("arrival_month") \
        .parquet(PROCESSED_PATH)
    logger.info(f"Pipeline complete. {cleaned_count} rows written successfully.")

except Exception as e:
    logger.error(f"LOAD failed: {str(e)}")
    raise

job.commit()
