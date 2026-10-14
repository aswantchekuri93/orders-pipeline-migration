"""Unit tests for the validation rules. They need only PySpark (no Delta, no Databricks).

Run from the databricks/ folder:   pytest -q
(On Windows use WSL, or run them in CI; Spark on plain Windows needs extra Hadoop files.)
"""
from orders_pipeline.rules import RAW_COLUMNS, latest_per_order, parse_and_validate

RAW = RAW_COLUMNS + ["_source_file", "_run_id"]


def _raw(spark, rows):
    return spark.createDataFrame([r + ("f.csv", "run1") for r in rows], ", ".join(f"{c} string" for c in RAW))


def _customers(spark):
    return spark.createDataFrame([("C001",), ("C002",)], "customer_id string")


def test_good_row_is_cleaned(spark):
    raw = _raw(spark, [("1", " c001 ", "10.50", "2026-10-01", "NEW", "2026-10-01 09:00:00")])
    valid, rejects = parse_and_validate(raw, _customers(spark))
    row = valid.collect()[0]
    assert rejects.count() == 0
    assert (row.order_id, row.customer_id, row.status) == (1, "C001", "new")
    assert str(row.amount) == "10.50"


def test_each_bad_row_gets_the_right_reason(spark):
    raw = _raw(
        spark,
        [
            ("", "c001", "5", "2026-10-01", "new", "2026-10-01 09:00:00"),       # no order id
            ("2", "c999", "5", "2026-10-01", "new", "2026-10-01 09:00:00"),      # unknown customer
            ("3", "c001", "abc", "2026-10-01", "new", "2026-10-01 09:00:00"),    # text amount
            ("4", "c001", "-1", "2026-10-01", "new", "2026-10-01 09:00:00"),     # negative amount
            ("5", "c001", "5", "2026-13-40", "new", "2026-10-01 09:00:00"),      # impossible date
            ("6", "c001", "5", "2026-10-01", "new", "not a time"),               # bad timestamp
        ],
    )
    valid, rejects = parse_and_validate(raw, _customers(spark))
    assert valid.count() == 0
    reasons = sorted(r.reject_reason for r in rejects.collect())
    assert reasons == [
        "invalid amount",
        "invalid amount",
        "invalid order_date",
        "invalid order_id",
        "invalid updated_at",
        "unknown customer_id",
    ]


def test_latest_version_of_an_order_wins(spark):
    df = spark.createDataFrame(
        [(7, "2026-10-01 09:00:00", 100), (7, "2026-10-01 10:00:00", 200)],
        "order_id long, updated_at string, amount long",
    ).selectExpr("order_id", "cast(updated_at as timestamp) as updated_at", "amount")
    out = latest_per_order(df).collect()
    assert len(out) == 1 and out[0].amount == 200
