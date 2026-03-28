ALTER TABLE oms_dead_letter
    ADD COLUMN IF NOT EXISTS account_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS order_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS event_type VARCHAR(64),
    ADD COLUMN IF NOT EXISTS event_at BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS source VARCHAR(64) NOT NULL DEFAULT 'gateway-audit';

CREATE TABLE IF NOT EXISTS oms_pending_orphan (
    id BIGSERIAL PRIMARY KEY,
    event_ref VARCHAR(256) NOT NULL,
    account_id VARCHAR(128),
    order_id VARCHAR(128),
    event_type VARCHAR(64) NOT NULL,
    reason VARCHAR(128) NOT NULL,
    payload TEXT NOT NULL,
    event_at BIGINT NOT NULL,
    recorded_at BIGINT NOT NULL,
    source VARCHAR(64) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS oms_pending_orphan_event_ref_uidx
    ON oms_pending_orphan (event_ref);

CREATE INDEX IF NOT EXISTS oms_pending_orphan_order_idx
    ON oms_pending_orphan (order_id, event_at);
