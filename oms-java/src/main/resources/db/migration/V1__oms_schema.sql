CREATE TABLE IF NOT EXISTS oms_orders (
    order_id VARCHAR(128) PRIMARY KEY,
    account_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    side VARCHAR(16) NOT NULL,
    type VARCHAR(32) NOT NULL,
    quantity INTEGER NOT NULL,
    price DOUBLE PRECISION,
    time_in_force VARCHAR(32) NOT NULL,
    expire_at BIGINT,
    status VARCHAR(32) NOT NULL,
    submitted_at BIGINT NOT NULL,
    filled_at BIGINT,
    execution_time_ms DOUBLE PRECISION,
    status_reason TEXT,
    filled_quantity BIGINT NOT NULL,
    remaining_quantity BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS oms_order_events (
    order_id VARCHAR(128) NOT NULL,
    event_ref VARCHAR(256) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    event_at BIGINT NOT NULL,
    label TEXT NOT NULL,
    detail TEXT NOT NULL,
    source VARCHAR(64) NOT NULL,
    PRIMARY KEY (order_id, event_ref)
);

CREATE INDEX IF NOT EXISTS oms_order_events_order_idx
    ON oms_order_events (order_id, event_at);

CREATE TABLE IF NOT EXISTS oms_reservations (
    reservation_id VARCHAR(128) PRIMARY KEY,
    account_id VARCHAR(128) NOT NULL,
    order_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    side VARCHAR(16) NOT NULL,
    reserved_quantity BIGINT NOT NULL,
    reserved_amount BIGINT NOT NULL,
    released_amount BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    opened_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS oms_reservations_account_idx
    ON oms_reservations (account_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS oms_event_dedup (
    event_id VARCHAR(256) PRIMARY KEY,
    aggregate_id VARCHAR(128) NOT NULL,
    aggregate_seq BIGINT NOT NULL,
    occurred_at BIGINT NOT NULL,
    recorded_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS oms_consumer_checkpoint (
    consumer_name VARCHAR(128) PRIMARY KEY,
    current_offset BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS oms_outbox (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id VARCHAR(128) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload TEXT NOT NULL,
    published_at BIGINT,
    created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS oms_outbox_publish_idx
    ON oms_outbox (published_at, id);

CREATE TABLE IF NOT EXISTS oms_dead_letter (
    id BIGSERIAL PRIMARY KEY,
    event_ref VARCHAR(256) NOT NULL,
    aggregate_id VARCHAR(128),
    reason VARCHAR(128) NOT NULL,
    detail TEXT,
    payload TEXT NOT NULL,
    recorded_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS oms_dead_letter_event_ref_idx
    ON oms_dead_letter (event_ref);
