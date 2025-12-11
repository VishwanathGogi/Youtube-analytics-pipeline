# -------------------------------------------------------------------
# PROJECT     : YouTube Trending Analytics Platform
# MODULE      : Lambda - Category Metadata Normalization
# PURPOSE     : Clean and flatten raw JSON ingested into the S3 raw zone.
#               Convert nested structures into tabular records and write
#               optimized Parquet outputs into the cleansed zone.
# RUNTIME     : AWS Lambda (Serverless ETL)
# AUTHOR      : Vishwanath Gogi
# -------------------------------------------------------------------

import awswrangler as wr
import pandas as pd
import urllib.parse
import os

# -------------------------------------------------------------------
# Environment variables passed from Lambda configuration.
# These allow the function to be deployed across multiple environments
# (dvt, qa, prod) without modifying the code.
# -------------------------------------------------------------------
S3_CLEANSED_PATH = os.environ['s3_cleansed_layer']
GLUE_DB_NAME     = os.environ['glue_catalog_db_name']
GLUE_TABLE_NAME  = os.environ['glue_catalog_table_name']
WRITE_MODE       = os.environ['write_data_operation']


def lambda_handler(event, context):
    """
    Lambda entry point.

    Trigger:
        • S3 PUT event on the raw data bucket for category JSON files.

    Responsibilities:
        1. Read raw nested JSON from S3.
        2. Extract and flatten the 'items' array.
        3. Write normalized data to S3 in Parquet format.
        4. Update AWS Glue Catalog for downstream Athena consumption.

    This function acts as the cleansing layer for category metadata.
    """

    # -------------------------------------------------------------------
    # Extract bucket name and file key from S3 trigger event.
    # -------------------------------------------------------------------
    bucket = event['Records'][0]['s3']['bucket']['name']
    key    = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        # -------------------------------------------------------------------
        # STEP 1: Read raw JSON from S3.
        #         Example structure:
        #         {
        #             "kind": "...",
        #             "items": [
        #                 {"id": "1", "snippet": {...}},
        #                 ...
        #             ]
        #         }
        # -------------------------------------------------------------------
        df_raw = wr.s3.read_json(f's3://{bucket}/{key}')

        # -------------------------------------------------------------------
        # STEP 2: Normalize nested payload.
        #         Flatten only the 'items' array, which contains the
        #         category-level metadata required for analytics.
        # -------------------------------------------------------------------
        df_flat = pd.json_normalize(df_raw['items'])

        # -------------------------------------------------------------------
        # STEP 3: Write normalized output to S3 as Parquet.
        #         • Partition-aware
        #         • Glue Catalog integrated
        #         • Supports append/overwrite based on WRITE_MODE
        # -------------------------------------------------------------------
        write_response = wr.s3.to_parquet(
            df=df_flat,
            path=S3_CLEANSED_PATH,
            dataset=True,
            database=GLUE_DB_NAME,
            table=GLUE_TABLE_NAME,
            mode=WRITE_MODE
        )

        # Return metadata for observability pipelines
        return write_response

    except Exception as e:
        # -------------------------------------------------------------------
        # Structured error logging. Output captured by CloudWatch Logs.
        # Helps debug file-level data issues or schema mismatches.
        # -------------------------------------------------------------------
        print(f"Exception encountered while processing {key} from bucket {bucket}")
        print(e)
        raise e