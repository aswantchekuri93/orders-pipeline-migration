-- Unity Catalog setup: the structure the pipeline expects.
-- Run once per environment (replace prod_orders with dev_orders, etc.).
-- On Databricks Free Edition you cannot create catalogs; use the existing one
-- (usually called `workspace`) and skip the CREATE CATALOG line.

-- 1) Catalog and one schema per layer ----------------------------------------
CREATE CATALOG IF NOT EXISTS prod_orders;
CREATE SCHEMA  IF NOT EXISTS prod_orders.bronze;
CREATE SCHEMA  IF NOT EXISTS prod_orders.silver;
CREATE SCHEMA  IF NOT EXISTS prod_orders.gold;
CREATE SCHEMA  IF NOT EXISTS prod_orders.ref;

-- 2) A volume for incoming files (replaces the S3 "incoming/" prefix) --------
CREATE VOLUME IF NOT EXISTS prod_orders.bronze.landing;
-- Files then live under /Volumes/prod_orders/bronze/landing/incoming/

-- 2b) If your files must stay in an existing S3 bucket, use an EXTERNAL LOCATION.
--     An admin first creates a storage credential (an IAM role Databricks can assume), then:
-- CREATE EXTERNAL LOCATION IF NOT EXISTS orders_landing
--   URL 's3://my-bucket/incoming/'
--   WITH (STORAGE CREDENTIAL orders_credential);
-- (This replaces the Glue job's IAM role access to the bucket.)

-- 3) Permissions (replaces IAM policies on the Glue role) --------------------
-- The pipeline runs as a service principal; engineers get read access.
-- GRANT USE CATALOG ON CATALOG prod_orders TO `orders-pipeline-sp`;
-- GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prod_orders.bronze TO `orders-pipeline-sp`;
-- GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prod_orders.silver TO `orders-pipeline-sp`;
-- GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prod_orders.gold   TO `orders-pipeline-sp`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA prod_orders.ref                          TO `orders-pipeline-sp`;
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME prod_orders.bronze.landing        TO `orders-pipeline-sp`;
-- GRANT USE CATALOG, USE SCHEMA, SELECT ON SCHEMA prod_orders.gold            TO `analysts`;

-- 4) Bring EXISTING Glue/S3 data across as Delta ------------------------------
-- (a) Parquet already on S3, convert in place (no data copy), then register it:
-- CONVERT TO DELTA parquet.`s3://my-bucket/curated/orders/` PARTITIONED BY (order_date);
-- (b) Or copy into a managed Delta table (cleanest; use for small/medium tables):
-- CREATE TABLE prod_orders.silver.orders AS
--   SELECT * FROM parquet.`s3://my-bucket/curated/orders/`;
-- (c) Reference data that lived in the Glue Catalog:
-- CREATE TABLE prod_orders.ref.customers AS
--   SELECT * FROM <old_glue_catalog_table>;   -- or load from CSV, see the notebook
