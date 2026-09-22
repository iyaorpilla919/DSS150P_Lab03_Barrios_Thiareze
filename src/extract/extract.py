import shutil
from pathlib import Path
from src.config import SETTINGS

SOURCE_FILES = ["customers.csv", "products.json", "orders.csv"]

def extract_sources(run_id: str) -> Path:
    base = Path(SETTINGS["paths"]["source_dir"])
    raw_root = Path(SETTINGS["paths"]["raw_dir"]) / f"run_id={run_id}"
    raw_root.mkdir(parents=True, exist_ok=True)
    for fname in SOURCE_FILES:
        src = base / fname
        if not src.exists():
            raise FileNotFoundError(f"[extract] missing source file: {src}")
        shutil.copy2(src, raw_root / fname)
    return raw_root