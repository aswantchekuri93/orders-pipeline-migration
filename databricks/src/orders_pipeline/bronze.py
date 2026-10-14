"""Step 1: INGEST. Copy newly arrived files into a raw ("bronze") Delta table.

Replaces: the Lambda handing a file path to Glue, plus Glue job bookmarks.

Auto Loader (format "cloudFiles") remembers which files it has already read, in
a checkpoint folder. Every run therefore picks up ONLY new files, however the
run was started (file arrival trigger, schedule, manual, retry).

Each row is stamped with the file it came from and the job run that loaded it,
so later steps can work on exactly this run's rows.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from orders_pipeline.config import Names
from orders_pipeline.rules import RAW_SCHEMA


def ingest_bronze(spark: SparkSession, names: Names, run_id: str) -> dict:
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(RAW_SCHEMA)
        .load(names.incoming_path)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.lit(run_id).alias("_run_id"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )

    # availableNow = "process what is new right now, then stop" (a batch-style run)
    (
        stream.writeStream.option("checkpointLocation", names.bronze_checkpoint)
        .trigger(availableNow=True)
        .toTable(names.bronze_table)
        .awaitTermination()
    )

    ingested = spark.table(names.bronze_table).filter(F.col("_run_id") == run_id).count()
    return {"rows_ingested": ingested}
