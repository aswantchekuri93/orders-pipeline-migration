# Migrating a pipeline from AWS (Lambda + Step Functions + Glue) to Databricks

A small but realistic pipeline, shown first the AWS way and then rebuilt on Databricks with
**Unity Catalog** and **Delta Lake**. Follow the steps here, then repeat them on your real
pipeline. The code lives in GitHub the same way yours does.

---

## 0. What has and has not been tested (read this first)

| Part | Status |
|---|---|
| Business rules and expected results (`reference_model.py`) | **Run and checked** (plain Python) |
| All Python files compile, no undefined names, YAML and JSON parse, task and file references resolve | **Checked** |
| The Spark / Delta / Auto Loader code and the notebook | **Not run yet.** Written carefully, but you will be the first to run it on Databricks |
| The bundle (`databricks.yml`, job file) | **Not validated against a real CLI.** A few key names (for example the file arrival trigger fields) follow the standard Jobs API. Run `databricks bundle validate` and fix anything it flags |
| GitHub Actions workflow | **Not run** |

If something fails, paste the error and the file name and it is usually a small fix.

---

## 1. The idea in one table

| In AWS today | On Databricks | File in this project |
|---|---|---|
| Lambda (S3 event or EventBridge schedule) | Job **trigger**: file arrival, or a schedule | `databricks/resources/orders_pipeline.job.yml` |
| Step Functions state machine | Job **tasks** with `depends_on` | same file |
| Step Functions `Catch` then SNS | A failed task stops later tasks, plus `email_notifications` | same file |
| Glue job "validate" | tasks `ingest_bronze` and `validate` | `src/orders_pipeline/bronze.py`, `silver.py`, `rules.py` |
| Glue job "process" | tasks `process_silver` and `publish_gold` | `src/orders_pipeline/silver.py`, `gold.py` |
| Glue job bookmarks | Auto Loader checkpoint + Delta `MERGE` | `bronze.py`, `silver.py` |
| S3 parquet files | **Delta tables** | everywhere |
| Glue Data Catalog | **Unity Catalog** (`catalog.schema.table`) | `sql/01_unity_catalog_setup.sql` |
| IAM roles and policies | Unity Catalog grants, storage credentials, service principals | same SQL file |
| CloudWatch logs | Run history in the Jobs UI plus a `pipeline_runs` audit table | `audit.py` |
| CloudFormation / CDK / Terraform deploy | **Databricks Asset Bundle** | `databricks/databricks.yml` |
| CodePipeline or Actions deploy | GitHub Actions running `databricks bundle deploy` | `.github/workflows/deploy.yml` |

```
AWS (before)                                   Databricks (after)
S3 file arrives                                file lands in a Unity Catalog volume
   |  Lambda                                      |  file arrival trigger
   v                                              v
Step Functions                                 ONE JOB, four tasks
   |--> Glue validate  --fail--> SNS              ingest_bronze --> validate --> process_silver --> publish_gold
   |--> Glue process                                                 (fails the run if too many bad rows)
   '--> SNS success                            bronze (raw) -> silver (clean, MERGE) -> gold (summary)
```

---

## 2. Two ways to follow along

**Path A: from GitHub, the way it works in real life (recommended)**
The code lives in a GitHub repo; Databricks pulls it into a **Git folder** and runs it from there.
1. **Put this project in GitHub.** Create a new repo on github.com (use **Public** for practice, and never put company data or secrets in it). Unzip the project, then on the repo page choose **Add file > Upload files**, drag in everything *inside* the unzipped `orders-pipeline-migration` folder (so `README.md` and `databricks/` sit at the top of the repo), and commit. Make sure the hidden `.github` folder came along too.
2. **Add it to Databricks.** In Databricks: **Workspace > Create > Git folder**, paste the repo URL, choose **GitHub**, and click **Create Git folder**. (For a private repo, first link your GitHub account under your user settings > Linked accounts.)
3. **Open the runner notebook** inside that Git folder: `databricks/notebooks/run_pipeline`. Connect to **Serverless**, set the **Catalog** box (usually `workspace`) and run the cells top to bottom.
4. The notebook does not contain the pipeline code. It imports it from `databricks/src/` in your repo, and reads the sample data from `data/`. Edit a file in the repo, pull it into the Git folder, and re-run to see your change.

**Path A-lite: one self-contained notebook (no GitHub needed)**
Import `notebook/orders_pipeline_notebook.py` (**Workspace > Import**). It has everything inlined, so use it only for a very quick look.

**Path B: the real deployment (bundle + GitHub Actions), in your work workspace**
See section 4, steps 9 and 10. Free Edition may not support all of this; your work workspace will.

You can also run `python reference_model.py` any time to see the answer key.

---

## 3. What is in the folder

```
data/incoming/            two sample order files (the "arrivals")
data/extra/               a deliberately broken file, to test the failure path
data/reference/           customers.csv (reference data)
aws_original/             BEFORE: Lambda, Step Functions JSON, two Glue jobs (reference only)
databricks/
  databricks.yml          bundle: dev and prod targets, variables
  resources/              the job: trigger, tasks, dependencies, alerts
  src/orders_pipeline/    the pipeline code (rules, bronze, silver, gold, audit, config)
  src/orders_pipeline/tasks/   one small entry file per job task
  notebooks/run_pipeline.py    runner notebook: imports src/ from the repo and runs everything
  sql/                    Unity Catalog setup and parity-check queries
  tests/                  unit tests for the rules
notebook/                 one-file notebook with everything inlined (no GitHub needed)
.github/workflows/        CI/CD (move to the repo root in your real repo)
reference_model.py        answer key in plain Python
```

---

## 4. The migration, step by step

Do these in order on your real pipeline. Each step says what to do, why, and where the example is.

### Step 1: Inventory the old pipeline
Before writing code, fill in one row per component:

| Component | Reads | Writes | Trigger | Depends on | Fails how | Who is told |
|---|---|---|---|---|---|---|
| Lambda `start-orders` | S3 event | Step Functions input | file arrives | none | retries | none |
| Glue `orders-validate` | `s3://.../incoming/` | staging + rejects | Step Functions | Lambda | raises error | SNS |
| ... | | | | | | |

Also list: IAM roles used, buckets, Glue Catalog tables, schedules, and anything downstream
(reports, other teams) that reads the output. Why: the migration is only finished when every row is replaced.

### Step 2: Design the target
Decide, on paper:
- **Layers:** bronze (raw as received), silver (validated and de-duplicated), gold (business summaries).
- **Names:** `<catalog>.<schema>.<table>`. One catalog per environment (`dev_orders`, `prod_orders`) keeps dev and prod separate.
- **Which Glue job steps become which tasks.** Keep the same boundaries first. Change logic later, not during the move.

### Step 3: Set up Unity Catalog
Run `databricks/sql/01_unity_catalog_setup.sql` (edit the names). It creates the schemas, a **volume**
for incoming files, example grants, and shows how to convert existing parquet data to Delta.
- No bucket changes needed? Use an **external location** (a storage credential that points to the S3 bucket) instead of a volume.
- Why: Unity Catalog replaces both the Glue Catalog and the IAM permissions. Access is granted on tables and volumes, not on buckets.

### Step 4: Land the data and the reference data
- Files arrive in `/Volumes/<catalog>/bronze/landing/incoming/`.
- Load reference data (customers) into `<catalog>.ref.customers` as a Delta table. The notebook shows how; for a one-off load:
  ```sql
  CREATE TABLE dev_orders.ref.customers AS
  SELECT * FROM read_files('/Volumes/dev_orders/bronze/landing/reference/customers.csv',
                           format => 'csv', header => true);
  ```
- Existing history on S3: convert or copy to Delta (options are in the setup SQL).

### Step 5: Convert the Glue "validate" job
Compare `aws_original/glue_validate_job.py` with `src/orders_pipeline/bronze.py`, `rules.py` and `silver.py (validate)`.

| In the Glue job | Becomes |
|---|---|
| `GlueContext`, `Job`, `job.commit()` | deleted; `spark` already exists |
| `getResolvedOptions` | command-line arguments (`argparse`) / job parameters |
| `create_dynamic_frame.from_options` + `transformation_ctx` (bookmark) | Auto Loader (`cloudFiles`) with a checkpoint |
| `ApplyMapping`, `ResolveChoice` | read as text, then `try_cast` in `rules.py` |
| rejected rows written without a reason | rejects table with a `reject_reason` column |
| `raise Exception` if too many bad rows | `RuntimeError` in `validate()` (the reject-rate gate) |

Keep the **rules the same** as the Glue job at first, so the outputs can be compared in step 10.

### Step 6: Convert the Glue "process" job
Compare `aws_original/glue_process_job.py` with `silver.py (process)` and `gold.py`.
- S3 parquet cannot be updated, so the Glue job overwrote partitions. A Delta table supports `MERGE`:
  insert new orders, update existing ones **only if the incoming row is newer**.
- Result: replaying a file or receiving an old correction late can no longer damage good data.

### Step 7: Convert Step Functions to job tasks
Open `databricks/resources/orders_pipeline.job.yml`.
- Each Step Functions `Task` state becomes a `task_key`.
- `Next:` becomes `depends_on:`. A task starts only after its dependencies succeed.
- `Catch` then SNS becomes `email_notifications: on_failure`. If `validate` fails, `process_silver` and `publish_gold` are skipped.
- Parameters such as the run date or file path become task parameters. `{{job.run_id}}` is the run's id; it tags each batch of rows so later tasks work on exactly that run's data.
- `max_concurrent_runs: 1` with `queue` keeps two triggers from overlapping.

### Step 8: Convert the Lambda trigger
There is no Lambda code to port. In the job file:
- **File arrival trigger** (used here): the job starts when a new file lands in the monitored folder.
- **Schedule:** replace the `trigger:` block with the commented `schedule:` block (cron, `Asia/Kolkata`).
- The trigger does **not** hand the job a file name. The job finds new files itself: that is Auto Loader's job (step 5).

### Step 9: Put it in GitHub as a bundle (Path B)
1. Install the Databricks CLI and log in: `databricks auth login --host <your workspace url>`.
2. Edit the `<placeholders>` in `databricks/databricks.yml` (workspace hosts, alert email, service principal).
3. From the `databricks/` folder:
   ```
   databricks bundle validate -t dev      # checks the configuration
   databricks bundle deploy   -t dev      # creates the job in your workspace
   ```
4. Put sample files in the incoming volume and run it once:
   ```
   databricks fs cp ../data/incoming/orders_2026-10-01.csv dbfs:/Volumes/dev_orders/bronze/landing/incoming/
   databricks bundle run orders_pipeline -t dev
   ```
   (`dev` mode pauses triggers on purpose, so you start runs by hand while testing.)
5. Push to GitHub. Add the secrets listed at the top of `.github/workflows/deploy.yml`.
   Pull requests then run the unit tests and `bundle validate`; merging to `main` deploys to prod.

The task files add a small `sys.path` line so `import orders_pipeline` works when Databricks runs
the file directly. If your imports fail on the first run, that line is where to look.
The production-grade alternative is to package `src/` as a **wheel** and use `python_wheel_task`.

### Step 10: Run old and new in parallel and compare
For a week or two, keep Glue running and let the Databricks job process the same input.
Use `databricks/sql/02_parity_checks.sql`: row counts, totals, `EXCEPT` queries in both directions,
and a comparison of the summary tables. **Every difference needs an explanation:** a bug, a deliberate
rule change (for example the new pipeline rejects unknown customers), or a timing gap.

### Step 11: Cut over and decommission
1. Point readers (reports, other teams) at the new gold/silver tables.
2. Pause the Lambda trigger and the Step Functions schedule; keep them one more cycle as a fallback.
3. Retire Glue jobs, the state machine and Lambda; remove unused IAM roles; delete old S3 staging data on a schedule you agree with the data owners.
4. Write down the rollback plan before the cutover, not after.

---

## 5. Things that behave differently (the common surprises)

- **Auto Loader remembers files, not names.** Re-running does not reprocess old files. To reprocess, you need a new checkpoint (a deliberate "backfill").
- **A failed validation still consumes the file.** The bad file was read into bronze and its rejects were saved, so a normal re-run will not retry it. Fix the file and re-deliver it under a new name, or use **Repair run** in the Jobs UI (same run id, so downstream tasks see the same rows).
- **File arrival trigger limits** (from the Databricks docs): new files only (overwriting a file with the same name does not trigger); no wildcards in the path; checks about every minute; without *file events* enabled on the location it is limited to about 10,000 files in the folder; you need `READ` on the location and `CAN MANAGE` on the job.
- **Staging table grows.** `silver.orders_staged` is appended each run. Add a retention step (delete old `_run_id`s) in a real pipeline.
- **Serverless has no `.cache()`.** The code avoids it. If your Glue code caches DataFrames, remove that.
- **Spark 4 is strict about casts** (ANSI mode). A bad cast raises an error instead of giving NULL, which is why the rules use `try_cast`.
- **Time zones.** Cron schedules take a `timezone_id`. EventBridge rules are written in UTC, so your old schedule may have been shifted; check what your consumers expect.
- **Costs change shape.** You stop paying per Glue DPU-hour and pay for Databricks compute instead. Compare using the real run times from the parallel-run period.

---

## 6. Checklist for your real pipeline

- [ ] Inventory complete, including downstream consumers
- [ ] Catalog, schemas, volume or external location, grants created
- [ ] Reference data and history loaded as Delta
- [ ] Each Glue job converted with **the same rules** as before
- [ ] Rejects kept with a reason; failure gate and alert tested with a bad file
- [ ] Job tasks, dependencies, trigger or schedule, alerts defined in a bundle
- [ ] Unit tests for the rules; CI validates the bundle on every pull request
- [ ] Prod runs as a service principal, not as a person
- [ ] Parallel run compared; differences explained
- [ ] Cutover and rollback plan agreed, old resources decommissioned

---

## 7. Exercises
1. Add a rule: `status` must be `new`, `shipped` or `cancelled`. Add a unit test. Re-run the notebook.
2. Add a `region` filter parameter to `publish_gold`.
3. Replace the full rebuild in `gold.py` with a `MERGE` of only the affected dates.
4. Add a retention step that deletes `orders_staged` rows older than 7 days.
5. Convert one of **your own** Glue jobs using the table in step 5, and compare outputs with the parity queries.
