# NHS A&E Synthetic Data Pipeline

An end-to-end AWS data pipeline processing NHS England synthetic A&E data.

## Architecture
```
S3 (Raw) → AWS Glue (Python/PySpark) → S3 (Processed) → Athena
```

## Tech Stack
- AWS Glue (ETL)
- AWS S3 (Storage)
- AWS Athena (Query)
- Python / PySpark

## Project Structure
- `scripts/` - Glue job scripts
- `data/` - Local sample data
- `tests/` - Data quality tests
- `docs/` - Architecture documentation
