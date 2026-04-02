# NHS A&E Synthetic Data Pipeline

An end-to-end AWS cloud data pipeline processing synthetic NHS England A&E attendance data, built as a portfolio project demonstrating modern data engineering practices.

## Architecture
```
S3 (Raw CSV)
    ↓
AWS Glue ETL Job (Python/PySpark)
- Data cleaning & standardisation
- Derived columns (4-hour breach flag, arrival hour/month)
- Data quality checks with assertions
    ↓
S3 (Processed Parquet, partitioned by arrival_month)
    ↓
AWS Glue Crawler (auto-detects schema)
    ↓
Glue Data Catalog (nhs_ae_db)
    ↓
Amazon Athena (SQL query layer)
```

## Dataset

Synthetic NHS A&E attendance data (50,000 records, 2023) with the following fields:

| Column | Type | Description |
|--------|------|-------------|
| attendance_id | int | Unique attendance identifier |
| arrival_datetime | timestamp | Date and time of arrival |
| departure_datetime | timestamp | Date and time of departure |
| wait_minutes | int | Total wait time in minutes |
| ae_department_type | string | Type 1 / Type 2 / Type 3 |
| arrival_mode | string | Ambulance / Walk-in / Other |
| age_band | string | Patient age group |
| gender | string | Patient gender |
| icd10_chapter | string | Diagnosis chapter code |
| admission_flag | int | 1 = admitted, 0 = discharged |
| provider_code | string | NHS Trust code |
| imd_decile | int | Deprivation decile (1=most deprived) |
| four_hour_breach | int | 1 = waited >4 hours (derived) |
| arrival_hour | int | Hour of arrival (derived) |
| arrival_month | int | Month of arrival (partition key) |

## Tech Stack

| Component | Technology |
|-----------|------------|
| Cloud | AWS |
| ETL | AWS Glue 4.0 (PySpark/Python 3) |
| Storage | Amazon S3 |
| Data Catalog | AWS Glue Data Catalog |
| Query Layer | Amazon Athena |
| Format | Parquet (snappy compressed, partitioned) |
| Version Control | Git / GitHub |

## Pipeline Features

- **Error handling** — try/except blocks at each pipeline stage (Extract, Transform, Load)
- **Data quality gates** — assertions fail the job if quality thresholds are breached
- **Logging** — structured logging throughout for observability
- **Partitioning** — output partitioned by arrival_month for query performance
- **Columnar format** — Parquet reduces storage and Athena query costs vs CSV

## Data Quality Checks

| Check | Threshold |
|-------|-----------|
| Minimum row count | ≥ 1,000 rows |
| Maximum rows removed | ≤ 10% of raw |
| Null gender after cleaning | 0 |
| Null age_band after cleaning | 0 |

## Sample Athena Queries
```sql
-- 4-hour breach rate by department type
SELECT
  ae_department_type,
  COUNT(*) as total_attendances,
  ROUND(AVG(wait_minutes), 1) as avg_wait_mins,
  ROUND(SUM(four_hour_breach) * 100.0 / COUNT(*), 1) as breach_pct
FROM nhs_ae_db.ae
GROUP BY ae_department_type
ORDER BY total_attendances DESC;

-- Attendances by hour of day
SELECT
  arrival_hour,
  COUNT(*) as attendances
FROM nhs_ae_db.ae
GROUP BY arrival_hour
ORDER BY arrival_hour;
```

## Infrastructure Setup

See `infrastructure/setup.sh` to recreate the AWS infrastructure from scratch.

## Author

Siu Cheung Lam (Chips)
[LinkedIn](https://www.linkedin.com/in/siu-cheung-lam)
