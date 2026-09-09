# Architecture

## Components

| Component | Responsibility |
|---|---|
| Raw S3 zone | Stores source category JSON and video-statistics CSV files |
| AWS Lambda | Normalizes category metadata after an S3 object-created event |
| AWS Glue Data Catalog | Provides source and cleansed table metadata |
| AWS Glue ETL | Transforms video statistics with PySpark |
| Cleansed S3 zone | Stores analytics-ready Parquet datasets |
| Amazon Athena | Queries cataloged data without managing servers |

## Category metadata flow

1. A JSON object is written to the configured raw S3 prefix.
2. S3 invokes the Lambda function.
3. Lambda validates the event, reads the object, and extracts the `items` array.
4. pandas normalizes nested category fields.
5. AWS SDK for pandas writes a Parquet dataset to the cleansed S3 location.
6. The write operation creates or updates Glue Catalog metadata.

## Video-statistics flow

1. A crawler or external deployment process registers raw CSV data in the Glue Catalog.
2. The Glue job reads the configured database and table.
3. Predicate pushdown limits the read to configured regions.
4. ApplyMapping enforces the analytical schema.
5. ResolveChoice handles ambiguous source types and DropNullFields removes null-only fields.
6. The job writes Parquet partitioned by `region` to the cleansed S3 location.

## Authentication boundary

The Python code contains no login system or long-lived credentials. Lambda and Glue are expected to authenticate to AWS through separate IAM execution roles. See [SECURITY.md](SECURITY.md).

## Operational behavior

- Lambda emits structured log records to CloudWatch through the standard runtime logger.
- Exceptions are re-raised so AWS records failed invocations.
- Glue calls `job.commit()` only after a successful write.
- Retry and dead-letter behavior must be configured in AWS; it is not defined by this repository.
