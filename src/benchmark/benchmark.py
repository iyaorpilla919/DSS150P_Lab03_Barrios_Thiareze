import json
import time
import statistics
import platform
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from src.config import SETTINGS
from src.load.load import _engine

def _median_time(fn, repeats=5):
    times, result = [], None
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    return statistics.median(times), result

def run_benchmark(run_id: str, repeats: int = 5) -> Path:
    curated_path = Path(SETTINGS["paths"]["curated_dir"]) / f"run_id={run_id}" / "sales_order_lines.parquet"
    df = pd.read_parquet(curated_path)

    bench_dir = Path(SETTINGS["paths"]["benchmark_dir"])
    bench_dir.mkdir(parents=True, exist_ok=True)
    csv_path, jsonl_path, parquet_path = (
        bench_dir / "curated.csv", bench_dir / "curated.jsonl", bench_dir / "curated.parquet"
    )
    rows = []

    w, _ = _median_time(lambda: df.to_csv(csv_path, index=False), repeats=1)
    r, _ = _median_time(lambda: pd.read_csv(csv_path), repeats=repeats)
    f, filt = _median_time(lambda: pd.read_csv(csv_path).query("status == 'DELIVERED'"), repeats=repeats)
    rows.append(dict(format="csv", size_bytes=csv_path.stat().st_size, write_s=w,
                      full_read_s=r, filtered_read_s=f, row_count=len(df), filtered_row_count=len(filt)))

    w, _ = _median_time(lambda: df.to_json(jsonl_path, orient="records", lines=True), repeats=1)
    r, _ = _median_time(lambda: pd.read_json(jsonl_path, lines=True), repeats=repeats)
    f, filt = _median_time(lambda: pd.read_json(jsonl_path, lines=True).query("status == 'DELIVERED'"), repeats=repeats)
    rows.append(dict(format="jsonl", size_bytes=jsonl_path.stat().st_size, write_s=w,
                      full_read_s=r, filtered_read_s=f, row_count=len(df), filtered_row_count=len(filt)))

    w, _ = _median_time(lambda: df.to_parquet(parquet_path, index=False, compression="snappy"), repeats=1)
    r, _ = _median_time(lambda: pd.read_parquet(parquet_path), repeats=repeats)
    f, filt = _median_time(lambda: pd.read_parquet(parquet_path).query("status == 'DELIVERED'"), repeats=repeats)
    rows.append(dict(format="parquet", size_bytes=parquet_path.stat().st_size, write_s=w,
                      full_read_s=r, filtered_read_s=f, row_count=len(df), filtered_row_count=len(filt)))

    engine = _engine()
    with engine.connect() as conn:
        pg_size = conn.execute(text("SELECT pg_total_relation_size('curated.sales_order_lines')")).scalar()
        r, _ = _median_time(lambda: pd.read_sql("SELECT * FROM curated.sales_order_lines", conn), repeats=repeats)
        f, filt_pg = _median_time(
            lambda: pd.read_sql("SELECT * FROM curated.sales_order_lines WHERE status = 'DELIVERED'", conn),
            repeats=repeats)
    rows.append(dict(format="postgresql", size_bytes=pg_size, write_s=None,
                      full_read_s=r, filtered_read_s=f, row_count=len(df), filtered_row_count=len(filt_pg)))

    out_csv = bench_dir / "benchmark_results.csv"
    pd.DataFrame(rows).to_csv(out_csv, index=False)
    context = {"python": platform.python_version(), "platform": platform.platform(),
               "processor": platform.processor(), "repeats": repeats}
    (bench_dir / "benchmark_context.json").write_text(json.dumps(context, indent=2))
    return out_csv

def write_partitioned(run_id: str) -> Path:
    curated_path = Path(SETTINGS["paths"]["curated_dir"]) / f"run_id={run_id}" / "sales_order_lines.parquet"
    df = pd.read_parquet(curated_path)
    part_root = Path(SETTINGS["paths"]["partitioned_dir"])
    part_root.mkdir(parents=True, exist_ok=True)
    df.to_parquet(part_root, partition_cols=["order_year", "order_month"], index=False)
    return part_root