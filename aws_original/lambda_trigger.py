"""
BEFORE (AWS): the Lambda that starts the pipeline. Reference only.

Two ways it was invoked:
  1. S3 "ObjectCreated" event  -> a new file arrived
  2. EventBridge schedule      -> run at a fixed time (e.g. every night)

DBX: both disappear as code. A Databricks job has a *trigger*:
       - file arrival trigger (watches a Unity Catalog volume / external location)
       - or a schedule (cron)
     See databricks/resources/orders_pipeline.job.yml
"""
import json
import os
from datetime import datetime, timezone

import boto3

sfn = boto3.client("stepfunctions")
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
DEFAULT_SOURCE = os.environ.get("DEFAULT_SOURCE", "s3://my-bucket/incoming/")


def handler(event, context):
    # Case 1: S3 event, so process the file(s) that just arrived
    if "Records" in event:
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            if not key.endswith(".csv"):
                continue  # ignore non-data files
            _start(f"s3://{bucket}/{key}")
    # Case 2: scheduled run, so process everything in the default folder
    else:
        _start(DEFAULT_SOURCE)


def _start(source_path: str):
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps({"source_path": source_path, "run_date": run_date}),
    )
