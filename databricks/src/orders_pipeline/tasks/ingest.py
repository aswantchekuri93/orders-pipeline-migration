"""Job task 1: ingest_bronze   (replaces: Lambda trigger handing a file to Glue + job bookmarks)"""
import argparse
import os
import sys

# Make `import orders_pipeline` work when Databricks runs this file directly.
# (The production-grade alternative is to build the code into a wheel; see README.)
_HERE = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
sys.path.insert(0, os.path.abspath(os.path.join(_HERE, "..", "..")))

from pyspark.sql import SparkSession  # noqa: E402

from orders_pipeline.bronze import ingest_bronze  # noqa: E402
from orders_pipeline.config import Names  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--run-id", required=True)
    a = p.parse_args()

    spark = SparkSession.builder.getOrCreate()
    print(ingest_bronze(spark, Names(a.catalog), a.run_id))


if __name__ == "__main__":
    main()
