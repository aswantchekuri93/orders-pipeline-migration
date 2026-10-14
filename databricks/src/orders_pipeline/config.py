"""Where everything lives in Unity Catalog.

Unity Catalog names are  <catalog>.<schema>.<table>.  We use one schema per layer:

  bronze  raw data exactly as it arrived          (+ the 'landing' volume for files)
  silver  validated, de-duplicated, trusted data  (+ rejects and audit tables)
  gold    business-ready summaries
  ref     reference / dimension data

Only the catalog changes between environments (dev_orders, prod_orders, ...).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Names:
    catalog: str

    # --- files (a Unity Catalog volume replaces the S3 prefix) ---
    @property
    def landing_root(self) -> str:
        return f"/Volumes/{self.catalog}/bronze/landing"

    @property
    def incoming_path(self) -> str:
        return f"{self.landing_root}/incoming"

    @property
    def bronze_checkpoint(self) -> str:
        # Auto Loader remembers which files it has read here (replaces Glue job bookmarks)
        return f"{self.landing_root}/_checkpoints/orders_bronze"

    # --- tables ---
    @property
    def bronze_table(self) -> str:
        return f"{self.catalog}.bronze.orders_raw"

    @property
    def staged_table(self) -> str:
        return f"{self.catalog}.silver.orders_staged"

    @property
    def silver_table(self) -> str:
        return f"{self.catalog}.silver.orders"

    @property
    def rejects_table(self) -> str:
        return f"{self.catalog}.silver.orders_rejects"

    @property
    def runs_table(self) -> str:
        return f"{self.catalog}.silver.pipeline_runs"

    @property
    def customers_table(self) -> str:
        return f"{self.catalog}.ref.customers"

    @property
    def gold_table(self) -> str:
        return f"{self.catalog}.gold.daily_sales"
