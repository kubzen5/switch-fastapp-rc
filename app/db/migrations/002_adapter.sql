-- A durable fence survives a writer crash / ambiguous Snowflake commit.
CREATE TABLE source_write_fences (
    source text PRIMARY KEY,
    blocked boolean NOT NULL DEFAULT false
);
CREATE TABLE source_quarantine (
    batch_id uuid NOT NULL REFERENCES sync_batches(batch_id),
    source text NOT NULL,
    source_updated_at timestamptz NOT NULL,
    order_key bigint NOT NULL,
    raw_record jsonb NOT NULL,
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, source_updated_at, order_key)
);
