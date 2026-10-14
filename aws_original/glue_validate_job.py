"""
BEFORE (AWS): Glue job #1 "orders-validate". Reference only (needs the Glue runtime).

What it does: read the new CSV, parse + validate rows, write good rows to a
staging S3 folder and bad rows to a rejects S3 folder, and FAIL the job if too
many rows are bad (which makes Step Functions take the Catch path).

DBX:  GlueContext / DynamicFrame / ApplyMapping / job bookmarks all go away.
      -> databricks/src/orders_pipeline/bronze.py   (Auto Loader replaces bookmarks)
      -> databricks/src/orders_pipeline/rules.py    (the validation rules)
      -> databricks/src/orders_pipeline/silver.py   (validate step + reject-rate gate)
"""
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping, ResolveChoice
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "run_date"])
MAX_REJECT_RATE = 0.25

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

raw = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["source_path"]]},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="raw",  # <- job bookmark: remembers files already read
)
mapped = ApplyMapping.apply(
    frame=raw,
    mappings=[
        ("order_id", "string", "order_id", "long"),
        ("customer_id", "string", "customer_id", "string"),
        ("amount", "string", "amount", "decimal(12,2)"),
        ("order_date", "string", "order_date", "date"),
        ("status", "string", "status", "string"),
        ("updated_at", "string", "updated_at", "timestamp"),
    ],
    transformation_ctx="mapped",
)
df = ResolveChoice.apply(frame=mapped, choice="make_cols").toDF()

# Customers come from the Glue Data Catalog
customers = (
    glueContext.create_dynamic_frame.from_catalog(database="ref", table_name="customers")
    .toDF()
    .select(F.upper(F.trim("customer_id")).alias("known_customer"))
)

df = df.withColumn("customer_id", F.upper(F.trim("customer_id")))
df = df.join(customers, df.customer_id == customers.known_customer, "left")
bad = df.filter(
    F.col("order_id").isNull()
    | F.col("amount").isNull() | (F.col("amount") <= 0)
    | F.col("order_date").isNull()
    | F.col("known_customer").isNull()
)
good = df.subtract(bad).drop("known_customer")  # (rejects carry no reason in this version)

total, n_bad = df.count(), bad.count()
good.write.mode("append").parquet(f"s3://my-bucket/staging/orders/run_date={args['run_date']}/")
bad.write.mode("append").parquet(f"s3://my-bucket/rejects/orders/run_date={args['run_date']}/")

if total and n_bad / total > MAX_REJECT_RATE:
    raise Exception(f"Too many bad rows: {n_bad}/{total}")  # -> Step Functions Catch

job.commit()
