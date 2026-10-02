-- Entity-only filtering must not depend on the leading source column.
CREATE INDEX event_deliveries_key_idx ON event_deliveries(entity_key, delivery_id);
CREATE INDEX event_rejections_delivery_idx ON event_rejections(delivery_id);
CREATE INDEX sync_runs_completed_idx ON sync_runs(source, finished_at DESC)
    WHERE status = 'completed';
