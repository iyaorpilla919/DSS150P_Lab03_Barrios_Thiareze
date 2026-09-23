# Goal 3 — Storage Benchmark Interpretation

I ran this benchmark on `run_id=bf1b3162bcdf`: 33,363 curated rows, 8,355 rows with `status = 'DELIVERED'`, 5 repeated reads per format, median reported. Full results are in `docs/evidence/benchmark_results.csv`.

Hardware/OS context: local Windows machine, Python 3.12.10 (local venv), pandas 2.2.2, pyarrow 16.1.0.

| Format | Size (bytes) | Write (s) | Full read (s) | Filtered read (s) |
|---|---|---|---|---|
| CSV | 10,604,968 | 1.010 | 0.223 | 0.234 |
| JSONL | 18,640,488 | 0.801 | 0.443 | 0.470 |
| Parquet | 3,931,983 | 0.196 | 0.038 | 0.046 |
| PostgreSQL | 8,953,856 (`pg_total_relation_size`) | n/a | 0.443 | 0.126 |

## 1. Which file format was smallest, and what explains it?

Parquet is smallest at 3.9 MB, versus CSV's 10.6 MB and JSONL's 18.6 MB. I store data in Parquet as typed, columnar values with per-column compression (snappy), so similar values compress efficiently and I don't pay for verbose text encoding. CSV stores everything as uncompressed plain text. JSONL is largest because it repeats every field's key name as a string on every single row, on top of also storing uncompressed text.

## 2. Which was fastest for a full read? Does that imply it's best for every workload?

Parquet reads fastest at 0.038s, roughly 6x faster than CSV's 0.223s and 11x faster than JSONL's 0.443s. That doesn't make it universally best. Parquet isn't human-readable, doesn't support simple line-by-line appends the way JSONL does for streaming ingestion, and requires a library like pyarrow rather than any plain text tool. I also saw Parquet read through PostgreSQL take 0.443s for the identical rows, slower than the raw Parquet file, which shows the fastest format depends on what overhead (network, query engine) sits on top of it.

## 3. How did filtered retrieval differ between Parquet and PostgreSQL? What additional PostgreSQL design could change the result?

Parquet's filtered read (0.046s) beats PostgreSQL's (0.126s) in absolute terms, but PostgreSQL's own filtered read is over 3x faster than its own full read (0.443s). I read the whole Parquet file into memory and filter with pandas afterward, while PostgreSQL pushes the `WHERE status = 'DELIVERED'` clause down to the server and returns only the 8,355 matching rows over the wire. Adding a B-tree index on `status` would likely narrow this gap further, since the server could then locate matching rows without scanning the full table.

## 4. Why is JSON Lines generally more pipeline-friendly than one giant JSON array for append/stream-oriented processing?

A single JSON array must stay syntactically closed and valid to parse at all, so I can't append a record without rewriting the closing bracket, and a partial write leaves the entire file unparseable. JSON Lines stores one complete, independent object per line, so I can append a new record as a single line write, a consumer can process the file line-by-line without loading it all, and a partial write still leaves every complete line before the crash point valid.

## 5. What happens if a partition key has extremely high cardinality or poor query locality?

A high-cardinality key, like `order_id`, would produce close to one partition per row: an explosion of tiny files, each carrying its own filesystem and metadata overhead, which can make directory listing and query planning slower than not partitioning at all. Poor query locality, a key queries rarely filter on, means I never actually use partition pruning, so I pay the file-count overhead without any I/O-skipping benefit. My own `order_year`/`order_month` scheme avoids both problems: it keeps partition count small (a few years, up to 12 months each) and matches exactly what my `load-partition --year --month` command filters on.