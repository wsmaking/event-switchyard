CREATE TABLE IF NOT EXISTS bo_account_overview (
    account_id VARCHAR(128) PRIMARY KEY,
    cash_balance BIGINT NOT NULL,
    available_buying_power BIGINT NOT NULL,
    reserved_buying_power BIGINT NOT NULL,
    position_count INTEGER NOT NULL,
    realized_pnl BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS bo_positions (
    account_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    net_qty BIGINT NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (account_id, symbol)
);

CREATE TABLE IF NOT EXISTS bo_order_states (
    order_id VARCHAR(128) PRIMARY KEY,
    account_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    side VARCHAR(16) NOT NULL,
    quantity BIGINT NOT NULL,
    working_price BIGINT NOT NULL,
    submitted_at BIGINT NOT NULL,
    last_event_at BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    filled_quantity BIGINT NOT NULL,
    reserved_amount BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS bo_order_states_account_idx
    ON bo_order_states (account_id, last_event_at DESC);

CREATE TABLE IF NOT EXISTS bo_fills (
    fill_id VARCHAR(128) PRIMARY KEY,
    order_id VARCHAR(128) NOT NULL,
    account_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    side VARCHAR(16) NOT NULL,
    quantity BIGINT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    notional BIGINT NOT NULL,
    liquidity VARCHAR(32) NOT NULL,
    filled_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS bo_fills_order_idx
    ON bo_fills (order_id, filled_at DESC);

CREATE INDEX IF NOT EXISTS bo_fills_account_idx
    ON bo_fills (account_id, filled_at DESC);

CREATE TABLE IF NOT EXISTS bo_ledger_entries (
    entry_id VARCHAR(128) PRIMARY KEY,
    event_ref VARCHAR(256) NOT NULL,
    account_id VARCHAR(128) NOT NULL,
    order_id VARCHAR(128) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    side VARCHAR(16) NOT NULL,
    quantity_delta BIGINT NOT NULL,
    cash_delta BIGINT NOT NULL,
    reserved_buying_power_delta BIGINT NOT NULL,
    realized_pnl_delta BIGINT NOT NULL,
    detail TEXT NOT NULL,
    event_at BIGINT NOT NULL,
    source VARCHAR(64) NOT NULL
);

CREATE INDEX IF NOT EXISTS bo_ledger_account_idx
    ON bo_ledger_entries (account_id, event_at DESC);

CREATE INDEX IF NOT EXISTS bo_ledger_order_idx
    ON bo_ledger_entries (order_id, event_at DESC);

CREATE TABLE IF NOT EXISTS bo_event_dedup (
    event_ref VARCHAR(256) PRIMARY KEY,
    recorded_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS bo_consumer_checkpoint (
    consumer_name VARCHAR(128) PRIMARY KEY,
    current_offset BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS bo_dead_letter (
    id BIGSERIAL PRIMARY KEY,
    event_ref VARCHAR(256) NOT NULL,
    account_id VARCHAR(128),
    order_id VARCHAR(128),
    event_type VARCHAR(64) NOT NULL,
    reason VARCHAR(128) NOT NULL,
    detail TEXT,
    payload TEXT NOT NULL,
    recorded_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS bo_dead_letter_event_ref_idx
    ON bo_dead_letter (event_ref);
