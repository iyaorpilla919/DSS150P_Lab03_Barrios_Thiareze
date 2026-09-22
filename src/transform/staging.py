from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd
from src.config import SETTINGS

RULES = SETTINGS["rules"]

def _now_utc():
    return datetime.now(timezone.utc)

def _write_quarantine(df, dataset_name, run_id):
    if df.empty:
        return
    q_dir = Path(SETTINGS["paths"]["quarantine_dir"]) / f"run_id={run_id}"
    q_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(q_dir / f"{dataset_name}_quarantine.csv", index=False)

def stage_customers(raw_dir: Path, run_id: str) -> pd.DataFrame:
    df = pd.read_csv(raw_dir / "customers.csv")
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    df = df.sort_values("updated_at").drop_duplicates("customer_id", keep="last")
    df["email_missing"] = df["email"].isna() | (df["email"].str.strip() == "")
    df["email"] = df["email"].str.strip().str.lower()
    df["city"] = df["city"].astype(str).str.strip().str.title()
    df["pipeline_run_id"] = run_id
    df["staged_at_utc"] = _now_utc()
    return df

def stage_products(raw_dir: Path, run_id: str) -> pd.DataFrame:
    raw = json.loads((raw_dir / "products.json").read_text())
    df = pd.json_normalize(raw)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    df = df.sort_values("updated_at").drop_duplicates("product_id", keep="last")
    df["category_name"] = df.get("category.name")
    df["category_department"] = df.get("category.department")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    invalid = df[df["unit_price"].isna() | (df["unit_price"] < 0)].copy()
    invalid["reason"] = "invalid_or_negative_price"
    _write_quarantine(invalid, "products", run_id)

    df = df[df["unit_price"].notna() & (df["unit_price"] >= 0)]
    df["pipeline_run_id"] = run_id
    df["staged_at_utc"] = _now_utc()
    return df

def stage_orders(raw_dir: Path, run_id: str) -> pd.DataFrame:
    df = pd.read_csv(raw_dir / "orders.csv")
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"], utc=True, errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce")

    bad_ts = df["order_timestamp"].isna() | df["updated_at"].isna()
    bad_qty = ~df["quantity"].between(RULES["quantity_min"], RULES["quantity_max"])
    bad_status = ~df["status"].isin(RULES["allowed_status"])
    bad_price = df["unit_price"].isna() | (df["unit_price"] < 0)
    invalid_mask = bad_ts | bad_qty | bad_status | bad_price

    invalid = df[invalid_mask].copy()
    invalid["reason"] = (
        bad_ts[invalid_mask].map({True: "bad_timestamp;"}).fillna("") +
        bad_qty[invalid_mask].map({True: "bad_quantity;"}).fillna("") +
        bad_status[invalid_mask].map({True: "bad_status;"}).fillna("") +
        bad_price[invalid_mask].map({True: "bad_price;"}).fillna("")
    )
    _write_quarantine(invalid, "orders", run_id)

    df = df[~invalid_mask].sort_values("updated_at").drop_duplicates("order_id", keep="last")
    df["pipeline_run_id"] = run_id
    df["staged_at_utc"] = _now_utc()
    return df

def run_staging(raw_dir: Path, run_id: str) -> dict:
    out_dir = Path(SETTINGS["paths"]["staging_dir"]) / f"run_id={run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    customers = stage_customers(raw_dir, run_id)
    products = stage_products(raw_dir, run_id)
    orders = stage_orders(raw_dir, run_id)
    customers.to_parquet(out_dir / "customers.parquet", index=False)
    products.to_parquet(out_dir / "products.parquet", index=False)
    orders.to_parquet(out_dir / "orders.parquet", index=False)
    return {"customers": len(customers), "products": len(products), "orders": len(orders)}