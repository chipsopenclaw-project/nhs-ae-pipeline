import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Init Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info("Job started: reading raw data from S3")

# -----------------------------------------------
# EXTRACT: Read raw CSV from S3
# -----------------------------------------------
df = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "s3://chips-pipeline-raw/ae/ae_synthetic_raw.csv"
)

raw_count = df.count()
logger.info(f"Raw row count: {raw_count}")

# -----------------------------------------------
# TRANSFORM: Data Cleaning
# -----------------------------------------------

# 1. Drop rows where critical fields are null
df_clean = df.dropna(subset=["attendance_id", "arrival_datetime", "ae_department_type"])

# 2. Remove invalid wait times (negative values)
df_clean = df_clean.filter(F.col("wait_minutes") >= 0)

# 3. Standardise gender - fill nulls with 'Unknown'
df_clean = df_clean.fillna({"gender": "Unknown", "age_band": "Unknown"})

# 4. Add derived column: 4-hour target breach flag (>240 mins = breach)
df_clean = df_clean.withColumn(
    "four_hour_breach",
    F.when(F.col("wait_minutes") > 240, 1).otherwise(0)
)

# 5. Add derived column: arrival hour for time-of-day analysis
df_clean = df_clean.withColumn(
    "arrival_hour",
    F.hour(F.col("arrival_datetime"))
)

# 6. Add derived column: arrival month
df_clean = df_clean.withColumn(
    "arrival_month",
    F.month(F.col("arrival_datetime"))
)

# -----------------------------------------------
# DATA QUALITY CHECKS
# -----------------------------------------------
cleaned_count = df_clean.count()
removed_count = raw_count - cleaned_count
breach_count = df_clean.filter(F.col("four_hour_breach") == 1).count()
breach_pct = round((breach_count / cleaned_count) * 100, 2)

logger.info(f"Cleaned row count: {cleaned_count}")
logger.info(f"Rows removed (quality issues): {removed_count}")
logger.info(f"4-hour breach rate: {breach_pct}%")

# Fail job if too many rows removed (>10% = something wrong)
if removed_count / raw_count > 0.10:
    raise Exception(f"Data quality check FAILED: {removed_count} rows removed ({round(removed_count/raw_count*100,1)}%)")

logger.info("Data quality checks PASSED")

# -----------------------------------------------
# LOAD: Write processed data to S3 as Parquet
# -----------------------------------------------
df_clean.write.mode("overwrite").partitionBy("arrival_month").parquet(
    "s3://chips-pipeline-processed/ae/"
)

logger.info("Pipeline complete: data written to chips-pipeline-processed")

job.commit()
