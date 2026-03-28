CREATE TABLE IF NOT EXISTS bo_aggregate_progress (
    aggregate_id VARCHAR(128) PRIMARY KEY,
    aggregate_seq BIGINT NOT NULL,
    event_ref VARCHAR(256),
    event_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
