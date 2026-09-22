from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from src.config import SETTINGS

def _engine():
    pg = SETTINGS["postgres"]
    url = f"postgresql+psycopg2://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['db']}"
    return create_engine(url)

UPSERT_SQL = text("""
INSERT INTO curated.sales_order_lines (
    order_id, customer_id, product_id, quantity, unit_price, discount_pct,
    gross_amount, discount_amount, net_amount, status, order_timestamp,
    order_year, order_month, source_updated_at, pipeline_run_id,
    processed_at_utc, record_hash
) VALUES (
    :order_id, :customer_id, :product_id, :quantity, :unit_price, :discount_pct,
    :gross_amount, :discount_amount, :net_amount, :status, :order_timestamp,
    :order_year, :order_month, :source_updated_at, :pipeline_run_id,
    :processed_at_utc, :record_hash
)
ON CONFLICT (order_id) DO UPDATE SET
    quantity = EXCLUDED.quantity, unit_price = EXCLUDED.unit_price,
    discount_pct = EXCLUDED.discount_pct, gross_amount = EXCLUDED.gross_amount,
    discount_amount = EXCLUDED.discount_amount, net_amount = EXCLUDED.net_amount,
    status = EXCLUDED.status, order_timestamp = EXCLUDED.order_timestamp,
    order_year = EXCLUDED.order_year, order_month = EXCLUDED.order_month,
    source_updated_at = EXCLUDED.source_updated_at, pipeline_run_id = EXCLUDED.pipeline_run_id,
    processed_at_utc = EXCLUDED.processed_at_utc, record_hash = EXCLUDED.record_hash
WHERE curated.sales_order_lines.record_hash <> EXCLUDED.record_hash;
""")

def load_curated(run_id: str) -> int:
    path = Path(SETTINGS["paths"]["curated_dir"]) / f"run_id={run_id}" / "sales_order_lines.parquet"
    df = pd.read_parquet(path)
    engine = _engine()
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(UPSERT_SQL, row.to_dict())
    return len(df)

def load_partition(year: int, month: int, run_id: str) -> int:
    part_path = Path(SETTINGS["paths"]["partitioned_dir"]) / f"order_year={year}" / f"order_month={month}"
    df = pd.read_parquet(part_path)
    engine = _engine()
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(UPSERT_SQL, row.to_dict())
        conn.execute(
            text("""INSERT INTO audit.partition_loads (order_year, order_month, pipeline_run_id, row_count)
                     VALUES (:y, :m, :rid, :n)"""),
            {"y": year, "m": month, "rid": run_id, "n": len(df)},
        )
    return len(df)