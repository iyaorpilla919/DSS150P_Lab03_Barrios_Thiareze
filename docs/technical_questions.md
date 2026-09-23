# Technical Questions

## 1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it?

`record_hash` tells the UPSERT whether a row's business content changed, so it can skip unchanged rows instead of rewriting them every run. I hash `order_id, customer_id, product_id, quantity, unit_price, discount_pct, status, order_timestamp`. I exclude `processed_at_utc`, `pipeline_run_id`, and `staged_at_utc` because they change every run regardless of the data, which would make the hash meaningless. I loaded the same 33,363 rows twice and confirmed `COUNT(*) = COUNT(DISTINCT order_id)` both times, proving it works.

## 2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?

Raw lets me fix and rerun staging/curated when a transformation rule is wrong, without re-acquiring the source data. This happened directly: I found `products.json` used `unit_price`, not the `price` field I originally assumed, fixed `staging.py` and `curated.py`, and reran both against the preserved raw snapshot. Raw also records exactly what the pipeline received on each run, independent of what the transformation code later did to it.

## 3. What is the difference between a data-quality rejection and a system exception?

A data-quality rejection happens when a record is readable but breaks a business rule, such as a bad price, invalid status, or orphan reference. I quarantine it with a reason and let the pipeline keep running; a normal run quarantined 63 orphaned rows this way. A system exception happens when the pipeline can't function at all, such as a missing file or refused connection. I saw this when a missing `orders.csv` raised `FileNotFoundError`, triggered retries, and failed the run. Quarantine logic can't fix this case, since there's no data to evaluate.

## 4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?

Parquet stores typed, columnar, compressed data; CSV stores untyped, uncompressed text that I must parse and cast on every read. In my benchmark, Parquet held the same 33,363 rows in 3.9 MB and read them in 0.038s; CSV took 10.6 MB and 0.223s.

## 5. Why is a DAG that contains all transformation logic directly considered harder to maintain?

Embedding logic in the DAG prevents me from testing or running it outside Airflow. My DAG only calls `python -m src.cli extract/run-all/load/validate`, the same commands I already verified locally. When Airflow-specific bugs surfaced later, a SQLAlchemy/pandas version conflict and a YAML indentation error, I immediately knew they belonged to the orchestration layer, not the pipeline logic, because I'd already proven the logic correct on its own.

## 6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.

A retry only produces a correct result if rerunning the task matches running it once, which idempotency guarantees. My `load` task upserts on `order_id` and checks `record_hash`, so retries never duplicate rows; I confirmed `COUNT(*) = COUNT(DISTINCT order_id)` held even after a deliberate failure-and-recovery cycle. Without conflict handling, a retry after a partial failure would either hit a primary-key violation, or, with no primary key at all, silently re-insert all 33,363 rows and double every downstream figure.

## 7. What trade-off is introduced by partitioning too aggressively?

Too many small partitions add filesystem and metadata overhead that outweighs any I/O savings. I partition by `order_year`/`order_month`, which keeps the count small (a few years, up to 12 months each). Partitioning by something high-cardinality, like `order_id`, would create mostly single-row partitions and slow scans and directory listings instead of speeding them up.

## 8. How would you adapt the pipeline if the source became an API or database instead of local files?

I'd change only `src/extract/extract.py`: instead of calling `shutil.copy2()`, `extract_sources()` would call the API or database and write the same run-specific raw snapshot to `data/raw/`. `staging.py`, `curated.py`, `load.py`, `validate.py`, and the Airflow DAG all read only from `data/raw/` and never care how it got there, so I wouldn't touch any of them.