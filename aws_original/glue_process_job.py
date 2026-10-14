"""
BEFORE (AWS): Glue job #2 "orders-process". Reference only (needs the Glue runtime).

What it does: read the validated rows, keep the latest version of each order,
and write the result to a curated S3 folder, then build a daily sales summary.

The painful part on Glue/S3: parquet files cannot be UPDATED. To apply a late
correction you must rewrite whole partitions, which is why the output below uses
overwrite and why a stale row could silently replace newer data.

DBX:  Delta tables support MERGE, so updates are safe and idempotent.
      -> databricks/src/orders_pipeline/silver.py  (process step: MERGE)
      -> databricks/src/orders_pipeline/gold.py    (daily sales summary)
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

staged = spark.read.parquet(f"s3://my-bucket/staging/orders/run_date={args['run_date']}/")

latest_first = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
latest = staged.withColumn("rn", F.row_number().over(latest_first)).filter("rn = 1").drop("rn")

# Overwrites the partitions it touches: no way to say "only if newer than what's stored"
latest.write.mode("overwrite").partitionBy("order_date").parquet("s3://my-bucket/curated/orders/")

customers = glueContext.create_dynamic_frame.from_catalog(
    database="ref", table_name="customers"
).toDF()
daily = (
    latest.filter("status <> 'cancelled'")
    .join(customers.select(F.upper(F.trim("customer_id")).alias("customer_id"), "region"), "customer_id")
    .groupBy("order_date", "region")
    .agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue"))
)
daily.write.mode("overwrite").parquet("s3://my-bucket/gold/daily_sales/")

job.commit()
