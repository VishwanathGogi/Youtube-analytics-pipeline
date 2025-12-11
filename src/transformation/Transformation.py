# -------------------------------------------------------------------
# PROJECT     : YouTube Trending Analytics Pipeline
# MODULE      : AWS Glue ETL - Raw Statistics → Cleansed Zone
# PURPOSE     : Transform raw YouTube CSV data from the Raw S3 Zone into
#               analytics-ready, partitioned Parquet files stored in the
#               Cleansed Zone. Normalizes schema, filters regions, and
#               updates AWS Glue Data Catalog for downstream Athena usage.
# AUTHOR      : Vishwanath Gogi
# RUNTIME     : AWS Glue (PySpark)
# -------------------------------------------------------------------

import sys
from awsglue.transforms import ApplyMapping, ResolveChoice, DropNullFields
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

# -------------------------------------------------------------------
# Glue Job Initialization
# -------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# Configuration
# Use predicate pushdown to reduce data scanned
# This filters only the regions we want (CA, GB, US)
# -------------------------------------------------------------------
PREDICATE = "region in ('ca', 'gb', 'us')"
SOURCE_DB = "de_youtube_raw"
SOURCE_TABLE = "raw_statistics"
TARGET_S3_PATH = "s3://de-on-youtube-cleansed-useast1-dvt/youtube/raw_statistics/"
PARTITION_KEYS = ["region"]

# -------------------------------------------------------------------
# STEP 1: Read Raw Data From Glue Catalog
# This loads the dataset registered by Glue Crawler.
# Predicate pushdown dramatically reduces scan size.
# -------------------------------------------------------------------
raw_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=SOURCE_TABLE,
    transformation_ctx="raw_dyf",
    push_down_predicate=PREDICATE
)

# -------------------------------------------------------------------
# STEP 2: Apply Schema Normalization
# Ensures consistent data types across the dataset.
# -------------------------------------------------------------------
mapped_dyf = ApplyMapping.apply(
    frame=raw_dyf,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "long"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "long"),
        ("likes", "long", "likes", "long"),
        ("dislikes", "long", "dislikes", "long"),
        ("comment_count", "long", "comment_count", "long"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string")
    ],
    transformation_ctx="mapped_dyf"
)

# -------------------------------------------------------------------
# STEP 3: Resolve Ambiguous Types
# Ensures data consistency for Athena and downstream analytics.
# -------------------------------------------------------------------
resolved_dyf = ResolveChoice.apply(
    frame=mapped_dyf,
    choice="make_struct",
    transformation_ctx="resolved_dyf"
)

# -------------------------------------------------------------------
# STEP 4: Drop Null Fields
# Removes structurally invalid records and cleans dataset.
# -------------------------------------------------------------------
clean_dyf = DropNullFields.apply(
    frame=resolved_dyf,
    transformation_ctx="clean_dyf"
)

# -------------------------------------------------------------------
# STEP 5: Coalesce Output to Reduce File Count
# Converts DynamicFrame → Spark DataFrame → back to DynamicFrame.
# This produces cleaner S3 output, easier for Athena partitions.
# -------------------------------------------------------------------
df = clean_dyf.toDF().coalesce(1)
final_dyf = DynamicFrame.fromDF(df, glue_context, "final_dyf")

# -------------------------------------------------------------------
# STEP 6: Write Transformed Data to Cleansed S3 Zone
# Writes Parquet files partitioned by region.
# -------------------------------------------------------------------
glue_context.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    connection_options={
        "path": TARGET_S3_PATH,
        "partitionKeys": PARTITION_KEYS
    },
    format="parquet",
    transformation_ctx="datasink"
)

# -------------------------------------------------------------------
# Finalize Glue Job
# -------------------------------------------------------------------
job.commit()