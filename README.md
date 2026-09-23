# DSS150P Lab 3 — Modular Data Pipeline

A reproducible, modular, rerun-safe, benchmarked, partitioned, and orchestrated pipeline for e-commerce sales order lines.

## Goal 1 — Reproducible Environment

**Setup:**
```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m src.cli validate-env

docker compose build pipeline
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

**Python version:** I run Python 3.12.10 locally, deliberately pinned after Python 3.14 (the machine's default) had no prebuilt wheels for `pandas==2.2.2`/`pyarrow==16.1.0`, which forced a slow, failure-prone source build. The Docker image uses `python:3.11-slim`. Both environments install the same pinned `requirements.txt` versions cleanly, which shows pinning, not matching Python minor versions, is what makes the environment reproducible.

**Config separation:** I keep non-secret defaults (paths, validation rules) in `config/settings.yml` and environment-specific values (Postgres credentials, host) in `.env` (untracked). `src/config.py` is the only file that merges the two into `SETTINGS`.

**Why `.venv/` isn't committed:** it's a machine-specific, regenerable artifact. `requirements.txt` is what makes the environment reproducible on another machine; the folder itself isn't portable and would bloat the repo.

**Evidence:** Docker build output, `docker compose ps` (postgres healthy), `\dn` schema listing (curated + audit), `git log` for the `goal1-reproducible-environment` branch.

## Goal 2 — ETL Pipeline

**Commands:**
```bash
python -m src.cli run-all
python -m src.cli load --run-id <RUN_ID>
python -m src.cli load --run-id <RUN_ID>
python -m src.cli validate --run-id <RUN_ID>
```

**Layer counts** (`run_id=bf1b3162bcdf`): raw copies 3 source files unmodified; staging produces 3,000 customers, 599 products, 33,426 orders; curated produces 33,363 rows; quarantine catches 63 orphaned order lines that reference a missing customer or product.

**Corrections I made to the provided starter data:** `products.json` uses `unit_price`, not `price`. `orders.csv` already carries `unit_price` and `discount_pct` per line, so `curated.py` uses those directly instead of overwriting them from a product join; I only join to products and customers to confirm the referenced IDs exist.

**Rerun-safety proof:**
```
 total | distinct_orders
-------+-----------------
 33363 |           33363
```
I confirmed this after loading the same run twice, and again after the Goal 4 failure/recovery test.

**`record_hash` excludes:** `processed_at_utc`, `pipeline_run_id`, `staged_at_utc`. These change every run regardless of business content, so including them would break rerun-safety (see `docs/technical_questions.md`, Q1).

**Error handling:** I keep quarantine (data-quality rejections like a bad price, invalid status, or orphan reference) separate from raised exceptions (pipeline/system failures like a missing source file). See `docs/technical_questions.md`, Q3.

**Evidence:** quarantine CSVs under `data/quarantine/run_id=.../`, the rerun-safety query result above, `python -m src.cli validate` output.

## Goal 3 — Storage & Partitioning

**Commands:**
```bash
python -m src.cli benchmark --run-id <RUN_ID> --repeats 5
python -m src.cli partition --run-id <RUN_ID>
python -m src.cli load-partition --year 2026 --month 1 --run-id <RUN_ID>
python -m src.cli load-partition --year 2026 --month 1 --run-id <RUN_ID>
```

See `docs/evidence/benchmark_results.csv` and `docs/goal3_interpretation.md` for full numbers and analysis. Parquet is smallest at 3.9 MB and fastest to read; CSV and JSONL are larger and slower; PostgreSQL's filtered query outperforms its own full scan through server-side predicate pushdown.

**Partition tree:**
```
data/partitioned/
  order_year=2025/order_month={1..12}/
  order_year=2026/order_month={1..9}/
```

**Selected partition load:** I loaded `year=2026, month=1` (1,684 rows) twice; both loads are audited in `audit.partition_loads` with distinct timestamps, and no duplicate `order_id` appears in `curated.sales_order_lines`.

**Evidence:** `docs/evidence/benchmark_results.csv`, `docs/goal3_interpretation.md`, partition folder listing, `audit.partition_loads` query result.

## Goal 4 — Airflow

**Commands:**
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d postgres airflow-webserver airflow-scheduler
```
Then trigger and manage runs from the Airflow UI at `http://localhost:8080`.

**Schedule:** `0 2 * * *` (daily, 02:00 UTC). I chose this off-peak time on the assumption that upstream source exports complete earlier in the day.

**Catchup:** `False`. Each run reprocesses the current state of `data/source/`, so I don't need to replay every missed daily interval for a lab dataset that doesn't change between runs.

**Run identity:** I pass Airflow's own `{{ run_id }}` as `--run-id` to every task (`extract`, `transform`, `load`, `validate`), so `pipeline_run_id` stays consistent across all four tasks in a single DAG run.

**Evidence:** `docs/evidence/`
- `goal4_fullrunevidence.png`: full manual run, all 4 tasks succeeded
- Partition run verified via `audit.partition_loads` (new row with `pipeline_run_id = manual__2026-09-23T00:33:55+00:00`, `row_count = 1684`)
- `goal4_failure_log.txt`, `goal4_failure_state.png`: deliberate failure test (renamed `orders.csv`). `extract` failed on attempt 3 of 3 after exhausting 2 retries; the log shows the `FileNotFoundError` and the `[FAILURE]` callback output
- `goal4_recovery_sucess.png`: I cleared and re-ran the same DAG run (`manual__2026-09-23T00:39:05+00:00`) after restoring the source file, and confirmed no duplicate rows resulted

**Note:** I also hit an unrelated real outage mid-Goal 4 (a combined `docker compose down` across both compose files stopped the pipeline's own Postgres container during troubleshooting) and recovered it by restarting the service. This is separate from, and in addition to, the required deliberate failure test above.

## Technical Reflection

**Modularity:** I kept `src/cli.py` as a thin composition layer; every subcommand calls into `extract/`, `transform/`, `load/`, `validate/`, or `benchmark/`. This let me reuse the same tested CLI commands verbatim inside the Airflow DAG's `BashOperator` tasks without duplicating any logic.

**Idempotency:** `load` and `load-partition` both upsert on `order_id` and check `record_hash`. I verified rerun-safety under three separate conditions: a manual double-load, a repeated partition load, and an Airflow task retry/recovery cycle. All three left `COUNT(*) = COUNT(DISTINCT order_id)`.

**Storage trade-offs:** No format wins on every axis. Parquet is smallest and fastest for local file access, but PostgreSQL's filtered query beats its own full-table read because it filters server-side. The best format depends on the access pattern, not just raw file size.

**Orchestration vs. business logic:** Airflow only sequences and retries `extract → transform → load → validate`; it never contains business rules itself. This separation meant every Airflow-layer bug I hit, including a dependency version conflict between Airflow's pinned SQLAlchemy/pandas and my own `requirements.txt`, YAML indentation errors, and a missing shared webserver secret key causing log-fetch 403s, stayed isolated to the orchestration layer and never required me to re-verify the pipeline logic, which I had already proven correct in Goals 1 through 3.

## Repository Structure

```
project/
├── .env.example
├── .gitignore
├── Dockerfile
├── Dockerfile.airflow
├── docker-compose.yml
├── docker-compose.airflow.yml
├── requirements.txt
├── config/settings.yml
├── data/
│   ├── source/          (tracked — provided input data)
│   ├── raw/              (regenerable, gitignored)
│   ├── staging/          (regenerable, gitignored)
│   ├── curated/          (regenerable, gitignored)
│   ├── quarantine/       (regenerable, gitignored)
│   ├── benchmarks/       (regenerable, gitignored)
│   └── partitioned/      (regenerable, gitignored)
├── src/
│   ├── extract/
│   ├── transform/
│   ├── load/
│   ├── validate/
│   └── benchmark/
├── dags/dss150p_pipeline.py
├── sql/init/001_schema.sql
├── docs/
│   ├── goal3_interpretation.md
│   ├── technical_questions.md
│   └── evidence/
└── README.md
```