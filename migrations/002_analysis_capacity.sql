CREATE TABLE IF NOT EXISTS analysis_jobs (
  token text PRIMARY KEY,
  user_id uuid NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
  slot integer NOT NULL UNIQUE CHECK (slot BETWEEN 1 AND 2),
  started_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS ticker_cache (
  ticker text PRIMARY KEY,
  fetched_at timestamptz NOT NULL,
  payload jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS operational_checks (
  name text PRIMARY KEY,
  ok boolean NOT NULL,
  checked_at timestamptz NOT NULL DEFAULT now(),
  detail text NOT NULL
);
