# Databricks notebook source
# MAGIC %md
# MAGIC # Run the pipeline from the GitHub repo
# MAGIC This notebook lives **inside the repo**. It does not contain the pipeline code. It imports it from
# MAGIC `databricks/src/orders_pipeline/` (the same files your GitHub repo holds), which is how real teams
# MAGIC test repo code on Databricks.
# MAGIC
# MAGIC **How to use it**
# MAGIC 1. In Databricks, add your GitHub repo as a **Git folder** (Workspace > Create > Git folder).
# MAGIC 2. Open this notebook from inside that Git folder, connect to **Serverless**, and run the cells top to bottom.
# MAGIC 3. Edit code in `src/`, commit and push (or pull) from the Git folder, then re-run from the top.
# MAGIC
# MAGIC In production the job runs the small files in `src/orders_pipeline/tasks/` instead of this notebook
# MAGIC (see `databricks/resources/orders_pipeline.job.yml`). This notebook is for trying things out.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Settings
# MAGIC * **Catalog**: usually `workspace` on Free Edition. See **Catalog** in the left menu.
# MAGIC * **Repo root**: leave empty. Only fill it in if the next cell cannot find your repo; then paste the path of the
# MAGIC   repo folder (for example `/Workspace/Users/you@example.com/my-repo`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("repo_root", "", "Repo root (optional)")

CATALOG = dbutils.widgets.get("catalog")
REPO_ROOT_OVERRIDE = dbutils.widgets.get("repo_root").strip()
print("Using catalog:", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Find the repo and make its code importable
# MAGIC The notebook sits in `<repo>/databricks/notebooks/`, so the repo root is two folders up.
# MAGIC Adding `<repo>/databricks/src` to Python's search path lets us write `from orders_pipeline... import ...`.

# COMMAND ----------

import os
import sys


def find_repo_root():
    if REPO_ROOT_OVERRIDE:
        return REPO_ROOT_OVERRIDE
    candidates = [os.getcwd()]
    try:  # the notebook's own location, if Databricks will tell us
        nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        candidates.append("/Workspace" + os.path.dirname(nb_path))
    except Exception:
        pass
    for folder in candidates:
        root = os.path.abspath(os.path.join(folder, "..", ".."))
        if os.path.isdir(os.path.join(root, "databricks", "src", "orders_pipeline")):
            return root
    raise RuntimeError(
        "Could not find the repo. Make sure you opened this notebook from inside the Git folder, "
        "or paste the repo folder path into the 'Repo root' box at the top and run this cell again. "
        f"Looked in: {candidates}"
    )


REPO_ROOT = find_repo_root()
SRC_DIR = os.path.join(REPO_ROOT, "databricks", "src")
DATA_DIR = os.path.join(REPO_ROOT, "data")
for p in (SRC_DIR, REPO_ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

print("Repo root :", REPO_ROOT)
print("Code      :", SRC_DIR)
print("Sample data:", DATA_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Import the pipeline code from the repo
# MAGIC The `reload` lines make sure you get your latest edits if you changed a file and pulled it into the Git folder
# MAGIC (Python otherwise keeps the old version in memory until the session restarts).

# COMMAND ----------

import importlib

import orders_pipeline.config as _config
import orders_pipeline.rules as _rules
import orders_pipeline.audit as _audit
import orders_pipeline.bronze as _bronze
import orders_pipeline.silver as _silver
import orders_pipeline.gold as _gold

for _m in (_config, _rules, _audit, _bronze, _silver, _gold):
    importlib.reload(_m)

from orders_pipeline.config import Names
from orders_pipeline.bronze import ingest_bronze
from orders_pipeline.silver import process, validate
from orders_pipeline.gold import publish

names = Names(CATALOG)
print(names.silver_table)
print(names.incoming_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Set up Unity Catalog objects and start from a clean slate
# MAGIC Same as `databricks/sql/01_unity_catalog_setup.sql`, in code. The DROP and delete lines make the notebook
# MAGIC safe to run again. (Never do this on real data. It is only for the practice catalog.)

# COMMAND ----------

for schema in ["bronze", "silver", "gold", "ref"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")

for t in [names.bronze_table, names.staged_table, names.silver_table, names.rejects_table,
          names.runs_table, names.gold_table, names.customers_table]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

dbutils.fs.rm(f"{names.landing_root}/_checkpoints", True)   # forget which files were read
dbutils.fs.rm(names.incoming_path, True)                    # empty the incoming folder
dbutils.fs.mkdirs(names.incoming_path)
print("Clean and ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load reference data and prepare to "deliver" files
# MAGIC The sample files come from the repo's `data/` folder. `land(...)` copies one into the incoming volume,
# MAGIC which is what the upstream system (and the old S3 event) did.

# COMMAND ----------

import shutil

os.makedirs(f"{names.landing_root}/reference", exist_ok=True)
shutil.copyfile(
    os.path.join(DATA_DIR, "reference", "customers.csv"),
    f"{names.landing_root}/reference/customers.csv",
)
(spark.read.option("header", "true").csv(f"{names.landing_root}/reference/customers.csv")
      .write.format("delta").saveAsTable(names.customers_table))


def land(relative_path):
    """Copy a sample file from the repo into the incoming volume."""
    src = os.path.join(DATA_DIR, relative_path)
    os.makedirs(names.incoming_path, exist_ok=True)
    shutil.copyfile(src, os.path.join(names.incoming_path, os.path.basename(src)))
    print("landed:", os.path.basename(src))


display(spark.table(names.customers_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The pipeline: four steps in order
# MAGIC In the real job these are four tasks (ingest, validate, process, publish). `run_id` is `{{job.run_id}}` there.
# MAGIC If `validate` raises an error, the steps after it never run. That is what `depends_on` does in the job.

# COMMAND ----------

def run_pipeline(run_id, max_reject_rate=0.25):
    out = {}
    out["ingest"] = ingest_bronze(spark, names, run_id)
    out["validate"] = validate(spark, names, run_id, max_reject_rate)
    out["process"] = process(spark, names, run_id)
    out["publish"] = publish(spark, names, run_id)
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. First file arrives
# MAGIC Expect 10 rows in, **8 valid, 2 rejected** (a negative amount, and an unknown customer `C999`).

# COMMAND ----------

land("incoming/orders_2026-10-01.csv")
print(run_pipeline("nb-run-1"))
display(spark.table(names.silver_table).orderBy("order_id"))
display(spark.table(names.rejects_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Second file arrives
# MAGIC Look for: order **1002** becomes `shipped`; order **1003** keeps **200.00** (the 999.00 row is older than what is stored);
# MAGIC order **1012** appears twice and the later version (**520.00**) wins; order **1013** is rejected.

# COMMAND ----------

land("incoming/orders_2026-10-02.csv")
print(run_pipeline("nb-run-2"))
display(spark.table(names.silver_table).orderBy("order_id"))
display(spark.table(names.gold_table).orderBy("order_date", "region"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Run again with no new file
# MAGIC Auto Loader remembers what it already read (this replaces Glue job bookmarks), so nothing changes.

# COMMAND ----------

before = spark.table(names.silver_table).count()
print(run_pipeline("nb-run-3"))
after = spark.table(names.silver_table).count()
print(f"silver rows before = {before}, after = {after}")
assert before == after, "a run with no new files changed the table"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Check against the answer key
# MAGIC `reference_model.py` is in the repo root. It works out the expected tables in plain Python, without Spark.
# MAGIC Two independent calculations agreeing is how you gain confidence in a migration.

# COMMAND ----------

import reference_model as rm

importlib.reload(rm)
exp = rm.expected_after_two_arrivals()

expected_silver = [
    (r["order_id"], r["customer_id"], str(r["amount"]), str(r["order_date"]), r["status"])
    for _, r in sorted(exp["silver"].items())
]
expected_gold = [
    (str(d), region, v["orders"], str(v["revenue"])) for (d, region), v in exp["gold"].items()
]
expected_rejects = {}
for _, reason in exp["rejects"]:
    expected_rejects[reason] = expected_rejects.get(reason, 0) + 1

actual_silver = [
    (r.order_id, r.customer_id, str(r.amount), str(r.order_date), r.status)
    for r in spark.table(names.silver_table).orderBy("order_id").collect()
]
actual_gold = [
    (str(r.order_date), r.region, r.orders, str(r.revenue))
    for r in spark.table(names.gold_table).orderBy("order_date", "region").collect()
]
actual_rejects = {
    r.reject_reason: r["count"]
    for r in spark.table(names.rejects_table).groupBy("reject_reason").count().collect()
}

assert actual_silver == expected_silver, actual_silver
assert actual_gold == expected_gold, actual_gold
assert actual_rejects == expected_rejects, actual_rejects
print("CHECK PASSED: silver, gold and rejects all match the answer key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. A bad file arrives: the pipeline must stop
# MAGIC 75% of this file's rows are bad, above the 25% limit. In AWS this was the Step Functions **Catch** sending an SNS message.
# MAGIC Here `validate` raises an error, `process` and `publish` never run, and the real job would email the alert address.
# MAGIC The silver table must stay unchanged, but the bad rows are still saved in the rejects table for investigation.

# COMMAND ----------

land("extra/orders_2026-10-04_broken.csv")
silver_before = spark.table(names.silver_table).count()
try:
    run_pipeline("nb-run-4")
    raise AssertionError("the broken file should have stopped the pipeline")
except RuntimeError as e:
    print("PIPELINE STOPPED (as intended):", e)
assert spark.table(names.silver_table).count() == silver_before
display(spark.table(names.rejects_table).filter("_run_id = 'nb-run-4'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Delta Lake extras: history, time travel, audit

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {names.silver_table}").select("version", "timestamp", "operation"))

# The silver table as it was after the FIRST file only
display(spark.sql(f"SELECT * FROM {names.silver_table} VERSION AS OF 0 ORDER BY order_id"))

# What each run did (replaces searching CloudWatch logs)
display(spark.table(names.runs_table).orderBy("logged_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try next: change code in the repo, then see it run
# MAGIC 1. In your Git folder, open `databricks/src/orders_pipeline/rules.py` and add a rule
# MAGIC    (for example: `status` must be new, shipped or cancelled).
# MAGIC 2. Re-run this notebook from the top. The `reload` in section 3 picks up your edit.
# MAGIC 3. Use the Git folder's **Git** button to commit and push your change to GitHub on a new branch.
# MAGIC 4. That branch, a pull request, and a deploy from `main` is the real workflow (see the README, steps 9 and 10).
