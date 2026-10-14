"""Tiny audit trail: one row per metric per run, in a Delta table.

Replaces digging through CloudWatch logs to answer "how many rows did last night's run reject?"
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from orders_pipeline.config import Names


def log_metrics(spark: SparkSession, names: Names, run_id: str, stage: str, metrics: dict) -> None:
    rows = [(run_id, stage, key, float(value)) for key, value in metrics.items()]
    if not rows:
        return
    (
        spark.createDataFrame(rows, "run_id string, stage string, metric string, value double")
        .withColumn("logged_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .saveAsTable(names.runs_table)
    )
