from pathlib import Path
import pandas as pd
from src.config import SETTINGS

class ValidationError(Exception):
    pass

def validate_curated(run_id: str):
    path = Path(SETTINGS["paths"]["curated_dir"]) / f"run_id={run_id}" / "sales_order_lines.parquet"
    df = pd.read_parquet(path)
    if df["order_id"].isna().any():
        raise ValidationError("[validate] null order_id in curated dataset")
    if df["order_id"].duplicated().any():
        raise ValidationError("[validate] duplicate order_id in curated dataset")
    if (df["net_amount"] < 0).any():
        raise ValidationError("[validate] negative net_amount found")
    print(f"[validate] OK - {len(df)} curated rows, 0 duplicate keys, 0 negative amounts")