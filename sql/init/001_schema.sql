CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS curated.sales_order_lines (
    order_id            TEXT PRIMARY KEY,
    customer_id         TEXT NOT NULL,
    product_id          TEXT NOT NULL,
    quantity            INTEGER NOT NULL,
    unit_price          NUMERIC(12,2) NOT NULL,
    discount_pct        NUMERIC(5,4) NOT NULL DEFAULT 0,
    gross_amount        NUMERIC(14,2) NOT NULL,
    discount_amount     NUMERIC(14,2) NOT NULL,
    net_amount           NUMERIC(14,2) NOT NULL,
    status               TEXT NOT NULL,
    order_timestamp      TIMESTAMPTZ NOT NULL,
    order_year           INTEGER,
    order_month          INTEGER,
    source_updated_at    TIMESTAMPTZ NOT NULL,
    pipeline_run_id      TEXT NOT NULL,
    processed_at_utc     TIMESTAMPTZ NOT NULL,
    record_hash          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit.partition_loads (
    id               SERIAL PRIMARY KEY,
    order_year       INTEGER NOT NULL,
    order_month      INTEGER NOT NULL,
    pipeline_run_id  TEXT NOT NULL,
    loaded_at_utc    TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_count        INTEGER NOT NULL
);