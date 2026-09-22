import hashlib
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from src.config import SETTINGS

HASH_COLUMNS = [
    "order_id", "customer_id", "product_id", "quantity",
    "unit_price", "discount_pct", "status", "order_timestamp",
]

def _now_utc():
    return datetime.now(timezone.utc)

def _record_hash(row) -> str:
    payload = "|".join(str(row[c]) for c in HASH_COLUMNS)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def run_curated(run_id: str) -> dict:
    stg_dir = Path(SETTINGS["paths"]["staging_dir"]) / f"run_id={run_id}"
    customers = pd.read_parquet(stg_dir / "customers.parquet")
    products = pd.read_parquet(stg_dir / "products.parquet")
    orders = pd.read_parquet(stg_dir / "orders.parquet")

    # Join only to check existence — do NOT pull unit_price from products;
    # orders.csv already has the authoritative per-line price.
    merged = orders.merge(customers[["customer_id"]], on="customer_id", how="left", indicator="cust_match")
    merged = merged.merge(products[["product_id"]], on="product_id", how="left", indicator="prod_match")

    orphan_mask = (merged["cust_match"] == "left_only") | (merged["prod_match"] == "left_only")
    orphans = merged[orphan_mask].copy()
    if not orphans.empty:
        orphans["reason"] = "orphan_customer_or_product_reference"
        q_dir = Path(SETTINGS["paths"]["quarantine_dir"]) / f"run_id={run_id}"
        q_dir.mkdir(parents=True, exist_ok=True)
        orphans.to_csv(q_dir / "curated_orphans.csv", index=False)

    curated = merged[~orphan_mask].copy()
    # unit_price and discount_pct already present from orders staging - no overwrite needed
    curated["gross_amount"] = curated["quantity"] * curated["unit_price"]
    curated["discount_amount"] = curated["gross_amount"] * curated["discount_pct"]
    curated["net_amount"] = curated["gross_amount"] - curated["discount_amount"]
    curated["order_year"] = curated["order_timestamp"].dt.year
    curated["order_month"] = curated["order_timestamp"].dt.month
    curated["source_updated_at"] = curated["updated_at"]
    curated["pipeline_run_id"] = run_id
    curated["processed_at_utc"] = _now_utc()
    curated["record_hash"] = curated.apply(_record_hash, axis=1)

    out_dir = Path(SETTINGS["paths"]["curated_dir"]) / f"run_id={run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    curated.to_parquet(out_dir / "sales_order_lines.parquet", index=False)
    return {"curated_rows": len(curated), "orphans_quarantined": len(orphans)}