"""AWS Glue job for transforming raw YouTube video statistics."""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping, DropNullFields, ResolveChoice
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


def optional_argument(name: str, default: str) -> str:
    """Read an optional Glue argument without making deployment fail."""
    flag = f"--{name}"
    if flag not in sys.argv:
        return default
    return getResolvedOptions(sys.argv, [name])[name]


required = getResolvedOptions(sys.argv, ["JOB_NAME", "TARGET_S3_PATH"])
source_db = optional_argument("SOURCE_DB", "de_youtube_raw")
source_table = optional_argument("SOURCE_TABLE", "raw_statistics")
regions = [
    region.strip().lower()
    for region in optional_argument("REGIONS", "ca,gb,us").split(",")
    if region.strip()
]
if not regions:
    raise ValueError("REGIONS must contain at least one region")

escaped_regions = ", ".join(f"'{region}'" for region in regions)
predicate = f"region in ({escaped_regions})"

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
job = Job(glue_context)
job.init(required["JOB_NAME"], required)

raw_frame = glue_context.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name=source_table,
    transformation_ctx="raw_frame",
    push_down_predicate=predicate,
)

mapped_frame = ApplyMapping.apply(
    frame=raw_frame,
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
        ("region", "string", "region", "string"),
    ],
    transformation_ctx="mapped_frame",
)

resolved_frame = ResolveChoice.apply(
    frame=mapped_frame,
    choice="make_struct",
    transformation_ctx="resolved_frame",
)
clean_frame = DropNullFields.apply(
    frame=resolved_frame,
    transformation_ctx="clean_frame",
)

# Forcing one output file is convenient for demos but does not scale. Override
# OUTPUT_PARTITIONS for larger datasets.
partition_count = int(optional_argument("OUTPUT_PARTITIONS", "1"))
if partition_count < 1:
    raise ValueError("OUTPUT_PARTITIONS must be at least 1")

data_frame = clean_frame.toDF()
if partition_count == 1:
    data_frame = data_frame.coalesce(1)
else:
    data_frame = data_frame.repartition(partition_count, "region")

final_frame = DynamicFrame.fromDF(data_frame, glue_context, "final_frame")
glue_context.write_dynamic_frame.from_options(
    frame=final_frame,
    connection_type="s3",
    connection_options={
        "path": required["TARGET_S3_PATH"],
        "partitionKeys": ["region"],
    },
    format="parquet",
    transformation_ctx="cleansed_sink",
)

job.commit()
