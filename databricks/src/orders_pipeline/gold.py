"""Step 4: PUBLISH. Build the business-ready daily sales summary ("gold").

Cancelled orders are excluded. This table is small, so we rebuild it fully on each
run, which is simple and safe to repeat. For big tables you would MERGE or
overwrite only the affected dates.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from orders_pipeline.audit import log_metrics
from orders_pipeline.config import Names


def publish(spark: SparkSession, names: Names, run_id: str) -> dict:
    if not spark.catalog.tableExists(names.silver_table):
        return {"gold_rows": 0}

    customers = spark.table(names.customers_table).select(
        F.upper(F.trim("customer_id")).alias("customer_id"), "region"
    )
    daily = (
        spark.table(names.silver_table)
        .filter("status <> 'cancelled'")
        .join(customers, "customer_id")
        .groupBy("order_date", "region")
        .agg(F.count("*").cast("long").alias("orders"), F.sum("amount").alias("revenue"))
    )
    daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        names.gold_table
    )

    stats = {"gold_rows": spark.table(names.gold_table).count()}
    log_metrics(spark, names, run_id, "publish", stats)
    return stats
