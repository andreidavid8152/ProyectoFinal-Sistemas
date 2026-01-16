CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS inventory_items (
  sku text PRIMARY KEY,
  quantity integer NOT NULL,
  price numeric(12,2) NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_errors (
  id bigserial PRIMARY KEY,
  file_name text NOT NULL,
  row_number integer NOT NULL,
  raw_row text NOT NULL,
  reason text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_ingest_runs (
  id bigserial PRIMARY KEY,
  file_name text NOT NULL,
  status text NOT NULL,
  total_rows integer NOT NULL,
  valid_rows integer NOT NULL,
  error_rows integer NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.inventory_summary (
  id bigserial PRIMARY KEY,
  run_at timestamptz NOT NULL DEFAULT now(),
  total_skus integer NOT NULL,
  total_quantity integer NOT NULL,
  total_value numeric(14,2) NOT NULL
);
