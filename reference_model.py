"""The ANSWER KEY for the sample pipeline, worked out with plain Python (no Spark).

Why it exists: when you migrate, you need an independent way to know what the output
SHOULD be. Run this file to print the expected tables for the sample data:

    python reference_model.py

It applies the same rules as databricks/src/orders_pipeline/rules.py, in the same order,
and replays the sample files as two separate pipeline runs (the way two file arrivals would).
"""
from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path(__file__).parent
MAX_REJECT_RATE = 0.25


def load_customers(path: Path) -> dict[str, str]:
    with open(path, newline="") as fh:
        return {r["customer_id"].strip().upper(): r["region"] for r in csv.DictReader(fh)}


def check_row(row: dict, customers: dict):
    """Return (parsed_row, None) or (None, reason). Same rule order as rules.py."""
    try:
        order_id = int(row["order_id"].strip())
    except ValueError:
        return None, "invalid order_id"

    customer = (row["customer_id"] or "").strip().upper()
    if not customer:
        return None, "missing customer_id"
    if customer not in customers:
        return None, "unknown customer_id"

    try:
        amount = Decimal(row["amount"].strip()).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None, "invalid amount"
    if amount <= 0:
        return None, "invalid amount"

    try:
        order_date = datetime.strptime(row["order_date"].strip(), "%Y-%m-%d").date()
    except ValueError:
        return None, "invalid order_date"

    try:
        updated_at = datetime.strptime(row["updated_at"].strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None, "invalid updated_at"

    return {
        "order_id": order_id,
        "customer_id": customer,
        "amount": amount,
        "order_date": order_date,
        "status": row["status"].strip().lower(),
        "updated_at": updated_at,
    }, None


def run_batch(files, customers, silver, rejects, max_reject_rate=MAX_REJECT_RATE):
    """One pipeline run over the files that just arrived. Mutates silver/rejects."""
    valid, bad = [], []
    for f in files:
        with open(f, newline="") as fh:
            for row in csv.DictReader(fh):
                parsed, reason = check_row(row, customers)
                (bad.append((row, reason)) if reason else valid.append(parsed))

    total = len(valid) + len(bad)
    rate = len(bad) / total if total else 0.0
    rejects.extend(bad)                      # rejects are kept even if the run fails
    if rate > max_reject_rate:
        raise RuntimeError(f"validation gate failed: {len(bad)}/{total} rejected")

    newest: dict[int, dict] = {}             # newest version of each order within the batch
    for r in valid:
        if r["order_id"] not in newest or r["updated_at"] > newest[r["order_id"]]["updated_at"]:
            newest[r["order_id"]] = r
    for oid, r in newest.items():            # MERGE: insert, or update only if newer
        if oid not in silver or r["updated_at"] > silver[oid]["updated_at"]:
            silver[oid] = r
    return {"rows_in": total, "valid": len(valid), "rejected": len(bad)}


def build_gold(silver, customers):
    agg = defaultdict(lambda: [0, Decimal("0.00")])
    for r in silver.values():
        if r["status"] == "cancelled":
            continue
        key = (r["order_date"], customers[r["customer_id"]])
        agg[key][0] += 1
        agg[key][1] += r["amount"]
    return {k: {"orders": v[0], "revenue": v[1]} for k, v in sorted(agg.items())}


def expected_after_two_arrivals():
    customers = load_customers(ROOT / "data" / "reference" / "customers.csv")
    silver, rejects = {}, []
    stats1 = run_batch([ROOT / "data" / "incoming" / "orders_2026-10-01.csv"], customers, silver, rejects)
    after_first = {k: dict(v) for k, v in silver.items()}
    stats2 = run_batch([ROOT / "data" / "incoming" / "orders_2026-10-02.csv"], customers, silver, rejects)
    return {
        "customers": customers,
        "stats": [stats1, stats2],
        "silver_after_first": after_first,
        "silver": silver,
        "rejects": rejects,
        "gold": build_gold(silver, customers),
    }


if __name__ == "__main__":
    exp = expected_after_two_arrivals()
    print("run stats:", exp["stats"])
    print("\nSILVER after the first arrival:", sorted(exp["silver_after_first"]))
    print("\nSILVER after both arrivals:")
    for oid in sorted(exp["silver"]):
        r = exp["silver"][oid]
        print(f"  {oid}  {r['customer_id']}  {r['amount']:>7}  {r['order_date']}  {r['status']:<9} {r['updated_at']}")
    print("\nREJECTS:")
    for row, reason in exp["rejects"]:
        print(f"  {reason:<20} order_id={row['order_id']!r:<6} amount={row['amount']!r}")
    print("\nGOLD daily_sales:")
    for (d, region), v in exp["gold"].items():
        print(f"  {d}  {region:<6} orders={v['orders']}  revenue={v['revenue']}")

    broken = ROOT / "data" / "extra" / "orders_2026-10-04_broken.csv"
    try:
        run_batch([broken], exp["customers"], dict(exp["silver"]), [])
    except RuntimeError as e:
        print(f"\nBroken file correctly stops the run: {e}")
