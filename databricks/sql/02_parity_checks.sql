-- Parity checks: prove the new pipeline gives the same answers as the old one.
-- Run these while BOTH pipelines are live (Phase "parallel run").
-- Assumes the old Glue output is readable in Databricks, for example as
-- `legacy.orders` (parquet on S3 registered as an external table / view).
-- Replace the table names with your own.

-- 1) Row counts
SELECT 'legacy' AS side, COUNT(*) AS row_count FROM legacy.orders
UNION ALL
SELECT 'new', COUNT(*) FROM prod_orders.silver.orders;

-- 2) Key totals
SELECT 'legacy' AS side, COUNT(DISTINCT order_id) AS orders, SUM(amount) AS total_amount FROM legacy.orders
UNION ALL
SELECT 'new', COUNT(DISTINCT order_id), SUM(amount) FROM prod_orders.silver.orders;

-- 3) Rows only in the OLD output (should normally be empty)
SELECT order_id, customer_id, amount, order_date, status FROM legacy.orders
EXCEPT
SELECT order_id, customer_id, amount, order_date, status FROM prod_orders.silver.orders;

-- 4) Rows only in the NEW output (should normally be empty)
SELECT order_id, customer_id, amount, order_date, status FROM prod_orders.silver.orders
EXCEPT
SELECT order_id, customer_id, amount, order_date, status FROM legacy.orders;

-- 5) Business-level check: the summary tables should agree
SELECT COALESCE(l.order_date, n.order_date) AS order_date,
       COALESCE(l.region, n.region)         AS region,
       l.revenue AS legacy_revenue, n.revenue AS new_revenue
FROM legacy.daily_sales l
FULL OUTER JOIN prod_orders.gold.daily_sales n
  ON l.order_date = n.order_date AND l.region = n.region
WHERE l.revenue IS NULL OR n.revenue IS NULL OR l.revenue <> n.revenue;

-- Every difference needs an explanation: a bug, an intentional rule change
-- (for example the new pipeline rejects unknown customers), or a timing gap.
