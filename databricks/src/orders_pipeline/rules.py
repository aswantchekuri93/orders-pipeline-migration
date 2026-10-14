"""The business rules, as pure DataFrame functions (no table reads or writes).

Keeping rules separate from I/O means you can unit test them with a tiny local
DataFrame, which is the main thing Glue scripts make hard.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Read everything as text first, then parse explicitly. This replaces Glue's
# ApplyMapping/ResolveChoice and lets bad values become rejects, not crashes.
RAW_SCHEMA = (
    "order_id string, customer_id string, amount string, "
    "order_date string, status string, updated_at string"
)
RAW_COLUMNS = ["order_id", "customer_id", "amount", "order_date", "status", "updated_at"]
LINEAGE_COLUMNS = ["_source_file", "_run_id"]
VALID_COLUMNS = [
    "order_id", "customer_id", "amount", "order_date", "status", "updated_at",
] + LINEAGE_COLUMNS


def parse_and_validate(raw: DataFrame, customers: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid_rows, rejected_rows).

    `raw` has RAW_COLUMNS plus _source_file and _run_id.
    try_cast / try_to_timestamp give NULL instead of an error. That matters on
    Spark 4 / recent Databricks runtimes, where a bad cast would fail the whole job.
    The first rule that fails becomes the reject reason.
    """
    known = customers.select(F.upper(F.trim("customer_id")).alias("_known_customer")).distinct()

    parsed = (
        raw.withColumn("p_order_id", F.expr("try_cast(trim(order_id) as bigint)"))
        .withColumn("p_customer_id", F.upper(F.trim("customer_id")))
        .withColumn("p_amount", F.expr("try_cast(trim(amount) as decimal(12,2))"))
        .withColumn(
            "p_order_date",
            F.expr("cast(try_to_timestamp(trim(order_date), 'yyyy-MM-dd') as date)"),
        )
        .withColumn("p_status", F.lower(F.trim("status")))
        .withColumn(
            "p_updated_at",
            F.expr("try_to_timestamp(trim(updated_at), 'yyyy-MM-dd HH:mm:ss')"),
        )
        .join(F.broadcast(known), F.col("p_customer_id") == F.col("_known_customer"), "left")
    )

    reason = (
        F.when(F.col("p_order_id").isNull(), "invalid order_id")
        .when(F.col("p_customer_id").isNull() | (F.col("p_customer_id") == ""), "missing customer_id")
        .when(F.col("_known_customer").isNull(), "unknown customer_id")
        .when(F.col("p_amount").isNull() | (F.col("p_amount") <= 0), "invalid amount")
        .when(F.col("p_order_date").isNull(), "invalid order_date")
        .when(F.col("p_updated_at").isNull(), "invalid updated_at")
    )
    parsed = parsed.withColumn("reject_reason", reason)

    valid = parsed.filter(F.col("reject_reason").isNull()).select(
        F.col("p_order_id").alias("order_id"),
        F.col("p_customer_id").alias("customer_id"),
        F.col("p_amount").alias("amount"),
        F.col("p_order_date").alias("order_date"),
        F.col("p_status").alias("status"),
        F.col("p_updated_at").alias("updated_at"),
        *LINEAGE_COLUMNS,
    )
    rejects = parsed.filter(F.col("reject_reason").isNotNull()).select(
        *RAW_COLUMNS, *LINEAGE_COLUMNS, "reject_reason"
    )
    return valid, rejects


def latest_per_order(df: DataFrame) -> DataFrame:
    """If an order appears more than once in a batch, keep its newest version."""
    newest_first = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    return df.withColumn("_rn", F.row_number().over(newest_first)).filter("_rn = 1").drop("_rn")
