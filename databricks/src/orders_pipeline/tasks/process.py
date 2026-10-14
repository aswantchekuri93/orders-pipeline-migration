"""Job task 3: process_silver   (replaces: Glue job "orders-process", now with a safe MERGE)"""
import argparse
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
sys.path.insert(0, os.path.abspath(os.path.join(_HERE, "..", "..")))

from pyspark.sql import SparkSession  # noqa: E402

from orders_pipeline.config import Names  # noqa: E402
from orders_pipeline.silver import process  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--run-id", required=True)
    a = p.parse_args()

    spark = SparkSession.builder.getOrCreate()
    print(process(spark, Names(a.catalog), a.run_id))


if __name__ == "__main__":
    main()
