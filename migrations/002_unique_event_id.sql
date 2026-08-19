-- Ensure event_id is unique across deliveries
DROP INDEX IF EXISTS idx_events_event_id;
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id ON events (event_id);
