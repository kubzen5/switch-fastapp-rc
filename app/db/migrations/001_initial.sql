CREATE TABLE sync_runs (
    run_id uuid PRIMARY KEY,
    source text NOT NULL,
    mode text NOT NULL CHECK (mode IN ('initial', 'incremental')),
    status text NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    high_watermark_at timestamptz NOT NULL,
    high_watermark_order_key bigint NOT NULL CHECK (high_watermark_order_key > 0),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz
);

CREATE TABLE sync_batches (
    batch_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES sync_runs(run_id),
    batch_sequence bigint NOT NULL CHECK (batch_sequence > 0),
    status text NOT NULL CHECK (status IN ('running', 'published', 'failed')),
    from_updated_at timestamptz,
    from_order_key bigint,
    to_updated_at timestamptz NOT NULL,
    to_order_key bigint NOT NULL CHECK (to_order_key > 0),
    records_seen integer NOT NULL DEFAULT 0 CHECK (records_seen >= 0),
    records_published integer NOT NULL DEFAULT 0 CHECK (records_published >= 0),
    error_code text,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE (run_id, batch_sequence),
    CHECK ((from_updated_at IS NULL) = (from_order_key IS NULL))
);

CREATE TABLE sync_state (
    source text PRIMARY KEY,
    last_updated_at timestamptz,
    last_order_key bigint CHECK (last_order_key > 0),
    last_completed_batch_id uuid REFERENCES sync_batches(batch_id),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((last_updated_at IS NULL) = (last_order_key IS NULL))
);

CREATE TABLE source_entity_versions (
    source text NOT NULL,
    entity_key text NOT NULL,
    source_version bigint NOT NULL CHECK (source_version > 0),
    source_updated_at timestamptz NOT NULL,
    last_event_id text NOT NULL,
    PRIMARY KEY (source, entity_key)
);

-- Every received delivery, including duplicates and invalid messages.
-- Bytea preserves the exact wire bytes, even when they are not valid JSON/UTF-8.
CREATE TABLE event_deliveries (
    delivery_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic text NOT NULL,
    partition_id integer NOT NULL,
    broker_offset bigint NOT NULL,
    message_key bytea,
    raw_message bytea,
    event_id text,
    source text,
    entity_key text,
    event_type text,
    occurred_at timestamptz,
    captured_at timestamptz,
    batch_id uuid,
    disposition text NOT NULL CHECK (disposition IN ('accepted', 'duplicate', 'stale', 'rejected')),
    processed_at timestamptz NOT NULL DEFAULT now()
);

-- Unique accepted domain versions; delivery audit above has no unique event_id.
CREATE TABLE processed_events (
    event_id text PRIMARY KEY,
    source text NOT NULL,
    entity_key text NOT NULL,
    source_version bigint NOT NULL CHECK (source_version > 0),
    content_hash text NOT NULL,
    envelope jsonb NOT NULL,
    first_delivery_id bigint NOT NULL REFERENCES event_deliveries(delivery_id),
    UNIQUE (source, entity_key, source_version)
);

CREATE TABLE orders_current (
    source text NOT NULL,
    entity_key text NOT NULL,
    source_version bigint NOT NULL CHECK (source_version > 0),
    source_updated_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    total_price numeric(18,2) NOT NULL CHECK (total_price >= 0),
    last_event_id text NOT NULL REFERENCES processed_events(event_id),
    last_captured_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, entity_key)
);

CREATE TABLE event_rejections (
    rejection_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id bigint NOT NULL REFERENCES event_deliveries(delivery_id),
    reason_code text NOT NULL,
    details jsonb NOT NULL DEFAULT '[]',
    rejected_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX event_deliveries_entity_idx ON event_deliveries(source, entity_key, delivery_id);
CREATE INDEX event_deliveries_time_idx ON event_deliveries(occurred_at, delivery_id);
CREATE INDEX event_deliveries_type_idx ON event_deliveries(event_type, delivery_id);

CREATE FUNCTION forbid_audit_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit tables are append-only';
END;
$$;

CREATE TRIGGER delivery_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON event_deliveries
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_audit_mutation();
CREATE TRIGGER rejection_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON event_rejections
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_audit_mutation();
CREATE TRIGGER processed_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON processed_events
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_audit_mutation();
